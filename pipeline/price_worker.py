import os
import json
import time
import csv
import requests
from typing import Dict, Optional, Tuple, Set
from decimal import Decimal
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_batch
from web3 import Web3

# Load environment
load_dotenv()

# Config
OUTPUT_DIR = os.getenv('OUTPUT_DIR', os.path.join(os.path.dirname(os.path.dirname(__file__)), 'output'))
os.makedirs(OUTPUT_DIR, exist_ok=True)
COINGECKO_ASSET_PLATFORM_ID = 'base'
REQUEST_TIMEOUT = int(os.getenv('REQUEST_TIMEOUT', '20'))
COIN_LIST_CACHE_PATH = os.path.join(OUTPUT_DIR, 'coingecko_coin_list.json')

# DB config
PIPELINE_SINK = os.getenv('PIPELINE_SINK', 'db').lower()  # db | csv | both
USE_DB = 'db' in PIPELINE_SINK
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')

# CSV schemas
CSV_COLUMNS_PRICE_TASKS = ['tokenContract','date']
CSV_COLUMNS_PRICES = ['tokenContract','date','usd']

ADDRESS_TO_ID_MAP: Dict[str, str] = {}


def get_db_connection():
    if not USE_DB:
        return None
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
        print('✅ Connected to PostgreSQL for price inserts.')
        return conn
    except Exception as e:
        print(f'⚠️ DB connection failed, continuing with CSV only: {e}')
        return None


def build_address_to_id_map():
    global ADDRESS_TO_ID_MAP
    try:
        print('\nBuilding address-to-id map from CoinGecko (with local cache)...')
        token_list = None
        # Use local cache if fresh (<24h)
        if os.path.exists(COIN_LIST_CACHE_PATH):
            mtime = os.path.getmtime(COIN_LIST_CACHE_PATH)
            if time.time() - mtime < 24*3600:
                with open(COIN_LIST_CACHE_PATH, 'r') as f:
                    token_list = json.load(f)
        if token_list is None:
            url = 'https://api.coingecko.com/api/v3/coins/list?include_platform=true'
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            token_list = resp.json()
            with open(COIN_LIST_CACHE_PATH, 'w') as f:
                json.dump(token_list, f)
        ADDRESS_TO_ID_MAP.clear()
        for token in token_list:
            platforms = token.get('platforms') or {}
            base_addr = platforms.get(COINGECKO_ASSET_PLATFORM_ID)
            if base_addr:
                try:
                    checksum = Web3.to_checksum_address(base_addr)
                    ADDRESS_TO_ID_MAP[checksum] = token['id']
                except Exception:
                    continue
        print(f'✅ Map built. {len(ADDRESS_TO_ID_MAP)} Base contracts mapped to CoinGecko IDs.')
    except Exception as e:
        print(f'⚠️ Could not build CoinGecko address map: {e}')


def get_historical_price(coingecko_id: Optional[str], date_str: str) -> Optional[float]:
    if not coingecko_id:
        return None
    try:
        url = f'https://api.coingecko.com/api/v3/coins/{coingecko_id}/history?date={date_str}'
        attempts = 0
        while True:
            attempts += 1
            try:
                resp = requests.get(url, timeout=REQUEST_TIMEOUT)
                break
            except requests.RequestException as re:
                if attempts >= 3:
                    raise re
                time.sleep(1.5 * attempts)
        resp.raise_for_status()
        data = resp.json()
        price = data.get('market_data', {}).get('current_price', {}).get('usd')
        # rate limit: free tier ~ 50-100/min; keep ~1.2s spacing
        time.sleep(1.2)
        return price
    except Exception as e:
        print(f'   ⚠️ Price fetch failed for {coingecko_id} @ {date_str}: {e}')
        return None


def read_price_tasks(path: str) -> Set[Tuple[str, str]]:
    tasks: Set[Tuple[str,str]] = set()
    if not os.path.exists(path):
        return tasks
    with open(path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            token = row.get('tokenContract')
            date = row.get('date')
            if token and date:
                try:
                    token = Web3.to_checksum_address(token)
                except Exception:
                    continue
                tasks.add((token, date))
    return tasks


def write_csv(file_path: str, records: list, columns: list, mode='a'):
    if not records:
        return
    is_new = not os.path.exists(file_path)
    with open(file_path, mode, newline='') as f:
        w = csv.DictWriter(f, fieldnames=columns)
        if is_new:
            w.writeheader()
        for r in records:
            w.writerow({c: r.get(c) for c in columns})


def upsert_prices_db(conn, rows: list):
    if not conn or not rows:
        return
    query = (
        'INSERT INTO prices (token_contract, date, usd) VALUES (%s, %s, %s) '
        'ON CONFLICT (token_contract, date) DO UPDATE SET usd = EXCLUDED.usd'
    )
    data = [(r['tokenContract'], r['date'], Decimal(str(r['usd'])) if r.get('usd') is not None else None) for r in rows]
    with conn.cursor() as cur:
        execute_batch(cur, query, data)
    conn.commit()
    print(f'✅ Upserted {len(rows)} rows into prices table.')


def main():
    print('🏁 Price worker starting...')
    build_address_to_id_map()
    tasks_path = os.path.join(OUTPUT_DIR, 'price_tasks.csv')
    prices_csv = os.path.join(OUTPUT_DIR, 'prices.csv')

    tasks = read_price_tasks(tasks_path)
    print(f'Found {len(tasks)} unique (token,date) price tasks to process.')

    db_conn = get_db_connection()

    processed = 0
    batch_rows = []
    for token, date_str in sorted(tasks):
        cg_id = ADDRESS_TO_ID_MAP.get(token)
        usd = get_historical_price(cg_id, date_str) if cg_id else None
        row = {'tokenContract': token, 'date': date_str, 'usd': usd}
        batch_rows.append(row)
        processed += 1
        # write CSV progressively to avoid data loss on interrupts
        if len(batch_rows) >= 20:
            write_csv(prices_csv, batch_rows, CSV_COLUMNS_PRICES)
            upsert_prices_db(db_conn, batch_rows)
            batch_rows.clear()
            print(f'...processed {processed}/{len(tasks)}')

    if batch_rows:
        write_csv(prices_csv, batch_rows, CSV_COLUMNS_PRICES)
        upsert_prices_db(db_conn, batch_rows)
        batch_rows.clear()

    if db_conn:
        db_conn.close()

    print('🏁 Price worker complete.')


if __name__ == '__main__':
    main()
