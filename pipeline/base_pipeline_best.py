import os
import json
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware
from dotenv import load_dotenv
import requests
from typing import List, Dict, Any, Optional
from datetime import datetime
import time
import psycopg2
from psycopg2.extras import execute_batch
from decimal import Decimal, getcontext

# Increase decimal precision for big-number math
getcontext().prec = 50

# --- SETUP & CONNECTIONS ---

load_dotenv()
QUICKNODE_URL = os.getenv("QUICKNODE_BASE_URL")

# Load DB connection info
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

if not QUICKNODE_URL:
    raise Exception("QUICKNODE_BASE_URL must be set in the .env file.")

w3 = Web3(Web3.HTTPProvider(QUICKNODE_URL))
w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

if not w3.is_connected():
    raise Exception("Failed to connect to the Base network via your RPC provider.")

print(f"✅ Successfully connected to Base network (Chain ID: {w3.eth.chain_id})")

# --- CONSTANTS & CACHES ---

TRANSFER_EVENT_TOPIC = w3.keccak(text="Transfer(address,address,uint256)").to_0x_hex()
ERC20_ABI = json.loads('[{"constant":true,"inputs":[],"name":"name","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"}]')
COINGECKO_ASSET_PLATFORM_ID = "base"
CHAIN = "base"
CONFIRMATIONS = 5  # avoid reorgs
REQUEST_TIMEOUT = 20  # seconds
COIN_LIST_CACHE_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "coin_list.json")

# Caches to minimize external API calls
TOKEN_METADATA_CACHE: Dict[str, Optional[Dict[str, Any]]] = {}
PRICE_CACHE: Dict[str, Optional[float]] = {}
ADDRESS_TO_ID_MAP: Dict[str, str] = {}

# --- DATABASE CONNECTION ---
def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        print("✅ Successfully connected to PostgreSQL database.")
        return conn
    except psycopg2.OperationalError as e:
        print(f"❌ Could not connect to the database: {e}")
        return None

def insert_transfers(conn, transfers: List[Dict[str, Any]]):
    """Batch inserts transfer data into the token_transfers table."""
    if not transfers:
        return
    query = """
        INSERT INTO token_transfers (
            transaction_hash, log_index, block_number, timestamp, chain,
            token_contract, token_symbol, from_address, to_address, value, usd_value
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (transaction_hash, log_index) DO NOTHING;
    """
    data_to_insert = []
    for t in transfers:
        data_to_insert.append(
            (
                t['transactionHash'],
                int(t['logIndex']),
                int(t['blockNumber']),
                int(t['timestamp']),
                t.get('chain', CHAIN),
                t['tokenContract'],
                t.get('tokenSymbol'),
                t['fromAddress'],
                t['toAddress'],
                Decimal(str(t.get('value'))) if t.get('value') is not None else None,
                Decimal(str(t.get('usdValue'))) if t.get('usdValue') is not None else None,
            )
        )
    with conn.cursor() as cursor:
        execute_batch(cursor, query, data_to_insert)
    conn.commit()
    print(f"✅ Inserted {len(data_to_insert)} records into token_transfers.")

def insert_swaps(conn, swaps: List[Dict[str, Any]]):
    """Batch inserts swap data into the dex_swaps table."""
    if not swaps:
        return
    query = """
        INSERT INTO dex_swaps (
            transaction_hash, log_index, block_number, timestamp, chain,
            pool_contract, token_in_contract, token_in_symbol, amount_in, usd_value_in,
            token_out_contract, token_out_symbol, amount_out, usd_value_out
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (transaction_hash, log_index) DO NOTHING;
    """
    data_to_insert = []
    for s in swaps:
        data_to_insert.append(
            (
                s['transactionHash'], int(s['logIndex']), int(s['blockNumber']), int(s['timestamp']), s.get('chain', CHAIN),
                s['poolContract'], s['tokenInContract'], s.get('tokenInSymbol'),
                Decimal(str(s.get('amountIn'))) if s.get('amountIn') is not None else None,
                Decimal(str(s.get('usdValueIn'))) if s.get('usdValueIn') is not None else None,
                s['tokenOutContract'], s.get('tokenOutSymbol'),
                Decimal(str(s.get('amountOut'))) if s.get('amountOut') is not None else None,
                Decimal(str(s.get('usdValueOut'))) if s.get('usdValueOut') is not None else None,
            )
        )
    with conn.cursor() as cursor:
        execute_batch(cursor, query, data_to_insert)
    conn.commit()
    print(f"✅ Inserted {len(data_to_insert)} records into dex_swaps.")


# --- DATA LOADING & MAPPING ---

def build_address_to_id_map():
    """
    Fetches the master token list from CoinGecko and builds a direct
    mapping from Base contract addresses to CoinGecko IDs.
    """
    global ADDRESS_TO_ID_MAP
    try:
        print("\nBuilding address-to-id map from CoinGecko (with local cache)...")
        token_list = None
        # Use local cache if exists and is fresh (< 24h)
        try:
            if os.path.exists(COIN_LIST_CACHE_PATH):
                mtime = os.path.getmtime(COIN_LIST_CACHE_PATH)
                if time.time() - mtime < 24 * 3600:
                    with open(COIN_LIST_CACHE_PATH, 'r') as f:
                        token_list = json.load(f)
        except Exception:
            token_list = None
        if token_list is None:
            url = "https://api.coingecko.com/api/v3/coins/list?include_platform=true"
            response = requests.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            token_list = response.json()
            # write cache best-effort
            try:
                with open(COIN_LIST_CACHE_PATH, 'w') as f:
                    json.dump(token_list, f)
            except Exception:
                pass
        for token in token_list:
            platforms = token.get('platforms', {})
            base_address = platforms.get(COINGECKO_ASSET_PLATFORM_ID)
            if base_address:
                try:
                    checksum_address = Web3.to_checksum_address(base_address)
                    ADDRESS_TO_ID_MAP[checksum_address] = token['id']
                except Exception:
                    continue
        print(f"✅ Map built successfully. Found {len(ADDRESS_TO_ID_MAP)} tokens on {COINGECKO_ASSET_PLATFORM_ID}.")
    except Exception as e:
        print(f"⚠️ Warning: Could not build CoinGecko address map. Price enrichment may fail. Error: {e}")

# --- CORE DATA FETCHING LOGIC ---

def get_block_with_receipts(block_number: int) -> Optional[Dict[str, Any]]:
    """Fetches block data and all its transaction receipts."""
    print(f"\nAttempting to fetch block and receipts for: {block_number}...")
    try:
        payload = json.dumps({
            "method": "eth_getBlockReceipts",
            "params": [
                hex(block_number) # "latest"
                ],
            "id": 1,
            "jsonrpc": "2.0"
        })
        headers = { 'Content-Type': 'application/json' }
        # Simple retry for transient RPC errors
        attempts = 0
        while True:
            attempts += 1
            try:
                response = requests.post(QUICKNODE_URL, headers=headers, data=payload, timeout=REQUEST_TIMEOUT)
                break
            except requests.RequestException as re:
                if attempts >= 3:
                    raise re
                time.sleep(1.5 * attempts)
        response.raise_for_status()
        data = response.json()
        if "error" in data:
            print(f"❌ RPC Error: {data['error']['message']}")
            return None
        # get block timestamp (retry via web3 if needed)
        block_info = w3.eth.get_block(block_number)
        result = {"receipts": data.get("result", []), "timestamp": block_info['timestamp']}
        print(f"✅ Found {len(result['receipts'])} receipts for block {block_number}.")
        return result
    except Exception as e:
        print(f"❌ Error in get_block_with_receipts: {e}")
        return None

# --- DATA PARSING & ENRICHMENT ---

def parse_and_enrich_transfers(block_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Parses, enriches with metadata, and adds USD value to transfers."""
    receipts = block_data.get('receipts', [])
    timestamp = block_data.get('timestamp')
    date_str = datetime.fromtimestamp(timestamp).strftime('%d-%m-%Y')
    
    print(f"\nParsing and enriching {len(receipts)} receipts from {date_str}...")
    enriched_transfers = []

    for receipt in receipts:
        for log in receipt.get('logs', []):
            if log.get('topics') and log['topics'][0] == TRANSFER_EVENT_TOPIC and len(log['topics']) > 2:
                try:
                    token_contract = Web3.to_checksum_address(log['address'])
                    metadata = get_token_metadata(token_contract)
                    if not metadata: continue

                    # Get historical price once per (token, date)
                    coingecko_id = ADDRESS_TO_ID_MAP.get(token_contract)
                    price = get_historical_price(coingecko_id, date_str) if coingecko_id else None

                    from_address = Web3.to_checksum_address('0x' + log['topics'][1][-40:])
                    to_address = Web3.to_checksum_address('0x' + log['topics'][2][-40:])
                    
                    log_data = log.get('data', '0x')
                    raw_value = 0 if log_data == '0x' else int(log_data, 16)
                    actual_value = (Decimal(raw_value) / (Decimal(10) ** Decimal(metadata['decimals'])))
                    usd_value = (actual_value * Decimal(str(price))) if price is not None else None

                    # Normalize indexes and hashes
                    log_index_hex = log.get('logIndex')
                    log_index = int(log_index_hex, 16) if isinstance(log_index_hex, str) and log_index_hex.startswith('0x') else int(log_index_hex)
                    block_number_hex = receipt.get('blockNumber')
                    block_number = int(block_number_hex, 16) if isinstance(block_number_hex, str) and block_number_hex.startswith('0x') else int(block_number_hex)
                    tx_hash = receipt['transactionHash']

                    enriched_transfers.append({
                        "blockNumber": block_number,
                        "timestamp": int(timestamp),
                        "chain": CHAIN,
                        "transactionHash": tx_hash,
                        "logIndex": log_index,
                        "tokenContract": token_contract,
                        "tokenSymbol": metadata['symbol'],
                        "fromAddress": from_address,
                        "toAddress": to_address,
                        "value": actual_value,
                        "usdValue": usd_value
                    })
                except Exception as e:
                    print(f"⚠️ Could not process a log. Error: {e}")

    print(f"✅ Fully enriched {len(enriched_transfers)} transfer events.")
    return enriched_transfers

def get_token_metadata(token_address: str) -> Optional[Dict[str, Any]]:
    """Fetches ERC-20 token metadata using a cache."""
    if token_address in TOKEN_METADATA_CACHE: return TOKEN_METADATA_CACHE[token_address]
    try:
        contract = w3.eth.contract(address=token_address, abi=ERC20_ABI)
        metadata = {"name": contract.functions.name().call(), "symbol": contract.functions.symbol().call(), "decimals": contract.functions.decimals().call()}
        TOKEN_METADATA_CACHE[token_address] = metadata
        return metadata
    except Exception:
        TOKEN_METADATA_CACHE[token_address] = None
        return None

def get_historical_price(coingecko_id: Optional[str], date_str: str) -> Optional[float]:
    """Gets historical price for a given CoinGecko ID on a specific date."""
    if not coingecko_id: return None

    cache_key = f"{coingecko_id}-{date_str}"
    if cache_key in PRICE_CACHE: return PRICE_CACHE[cache_key]

    try:
        print(f"    Fetching price for {coingecko_id} on {date_str}...")
        url = f"https://api.coingecko.com/api/v3/coins/{coingecko_id}/history?date={date_str}"
        attempts = 0
        while True:
            attempts += 1
            try:
                response = requests.get(url, timeout=REQUEST_TIMEOUT)
                break
            except requests.RequestException as re:
                if attempts >= 3:
                    raise re
                time.sleep(1.5 * attempts)
        response.raise_for_status()
        data = response.json()
        price = data.get('market_data', {}).get('current_price', {}).get('usd')
        PRICE_CACHE[cache_key] = price
        time.sleep(1.2) # Respect CoinGecko's free tier rate limit
        return price
    except Exception:
        PRICE_CACHE[cache_key] = None
        return None

# --- MAIN EXECUTION ---

def get_last_processed_block(conn, chain: str) -> Optional[int]:
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT last_processed_block FROM pipeline_state WHERE chain = %s", (chain,))
            row = cur.fetchone()
            return int(row[0]) if row else None
    except Exception:
        return None

def set_last_processed_block(conn, chain: str, block_number: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline_state (chain, last_processed_block)
            VALUES (%s, %s)
            ON CONFLICT (chain) DO UPDATE SET last_processed_block = EXCLUDED.last_processed_block
            """,
            (chain, int(block_number))
        )
    conn.commit()


if __name__ == "__main__":
    build_address_to_id_map()
    db_conn = get_db_connection()
    if not db_conn:
        exit()

    try:
        latest_block_number = w3.eth.block_number
        target_latest = max(0, latest_block_number - CONFIRMATIONS)
        print(f"\nLatest block on Base: {latest_block_number}. Target up to: {target_latest} (confirmations: {CONFIRMATIONS})")

        start_block = get_last_processed_block(db_conn, CHAIN)
        if start_block is None:
            start_block = target_latest  # start from tip (no backfill on first run)
        else:
            start_block = start_block + 1

        if start_block > target_latest:
            print("No new blocks to process.")
        else:
            for block_number in range(start_block, target_latest + 1):
                block_data = get_block_with_receipts(block_number)
                if not block_data:
                    continue
                enriched_transfers = parse_and_enrich_transfers(block_data)
                if enriched_transfers:
                    insert_transfers(db_conn, enriched_transfers)
                # TODO: parse swaps from receipts and insert via insert_swaps
                set_last_processed_block(db_conn, CHAIN, block_number)
                print(f"Processed block {block_number}.")

        print("\nPipeline run complete.")

    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")
    finally:
        if db_conn:
            db_conn.close()
            print("\nDatabase connection closed.")
