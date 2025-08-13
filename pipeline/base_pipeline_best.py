import os
import json
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware
from dotenv import load_dotenv
import requests
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
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
UNIV2_SWAP_TOPIC = w3.keccak(text="Swap(address,uint256,uint256,uint256,uint256,address)").to_0x_hex()
UNIV2_SWAP_TOPIC_ALT = w3.keccak(text="Swap(address,address,uint256,uint256,uint256,uint256)").to_0x_hex()
UNIV3_SWAP_TOPIC = w3.keccak(text="Swap(address,address,int256,int256,uint160,uint128,int24)").to_0x_hex()

# Output sink configuration
PIPELINE_SINK = os.getenv('PIPELINE_SINK', 'db').lower()  # db | csv | both
USE_DB = 'db' in PIPELINE_SINK
USE_CSV = 'csv' in PIPELINE_SINK or PIPELINE_SINK == 'both'
OUTPUT_DIR = os.getenv('OUTPUT_DIR', os.path.join(os.path.dirname(os.path.dirname(__file__)), 'output'))
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Price enrichment mode: inline (default) or deferred
PRICE_MODE = os.getenv('PRICE_MODE', 'inline').lower()  # inline | deferred
DEFER_PRICES = PRICE_MODE == 'deferred'

# CSV column orders matching DB schema
CSV_COLUMNS_TRANSFERS = [
    'blockNumber','timestamp','chain','transactionHash','logIndex',
    'tokenContract','tokenSymbol','fromAddress','toAddress','value','usdValue'
]
CSV_COLUMNS_SWAPS = [
    'blockNumber','timestamp','chain','transactionHash','logIndex','poolContract',
    'tokenInContract','tokenInSymbol','amountIn','usdValueIn',
    'tokenOutContract','tokenOutSymbol','amountOut','usdValueOut'
]

# CSV schemas for registry/queue
CSV_COLUMNS_TOKENS = [
    'tokenContract','symbol','decimals','firstSeenBlock','firstSeenAt'
]
CSV_COLUMNS_PRICE_TASKS = [
    'tokenContract','date'
]

# Caches to minimize external API calls
TOKEN_METADATA_CACHE: Dict[str, Optional[Dict[str, Any]]] = {}
PRICE_CACHE: Dict[str, Optional[float]] = {}
ADDRESS_TO_ID_MAP: Dict[str, str] = {}
POOL_TOKEN_CACHE: Dict[str, Optional[Dict[str, str]]] = {}
PRICE_TASKS_SET = set()  # in-memory dedupe per run

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

def insert_transfers(conn, block_number: int, transfers: List[Dict[str, Any]]):
    """Write transfers to DB and/or CSV according to PIPELINE_SINK."""
    if not transfers:
        return
    # CSV
    if USE_CSV:
        transfers_csv_path = os.path.join(OUTPUT_DIR, f'token_transfers_{block_number}.csv')
        write_csv(transfers_csv_path, transfers, CSV_COLUMNS_TRANSFERS)
        print(f"📝 Wrote {len(transfers)} transfer records to {transfers_csv_path}.")
    # DB
    if USE_DB and conn:
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

def insert_swaps(conn, block_number: int, swaps: List[Dict[str, Any]]):
    """Write swaps to DB and/or CSV according to PIPELINE_SINK."""
    if not swaps:
        return
    # CSV
    if USE_CSV:
        swaps_csv_path = os.path.join(OUTPUT_DIR, f'dex_swaps_{block_number}.csv')
        write_csv(swaps_csv_path, swaps, CSV_COLUMNS_SWAPS)
        print(f"📝 Wrote {len(swaps)} swap records to {swaps_csv_path}.")
    # DB
    if USE_DB and conn:
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
                    s['transactionHash'],
                    int(s['logIndex']),
                    int(s['blockNumber']),
                    int(s['timestamp']),
                    s.get('chain', CHAIN),
                    s['poolContract'],
                    s['tokenInContract'],
                    s.get('tokenInSymbol'),
                    Decimal(str(s.get('amountIn'))) if s.get('amountIn') is not None else None,
                    Decimal(str(s.get('usdValueIn'))) if s.get('usdValueIn') is not None else None,
                    s['tokenOutContract'],
                    s.get('tokenOutSymbol'),
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
        receipts = data.get('result')
        if receipts is None:
            print(f"No receipts returned for block {block_number}.")
            return None
        # Fetch block timestamp for enrichment
        try:
            b = w3.eth.get_block(block_number)
            ts = int(b.timestamp)
        except Exception:
            ts = int(time.time())
        print(f"✅ Found {len(receipts)} receipts for block {block_number}.")
        return {
            'blockNumber': block_number,
            'timestamp': ts,
            'receipts': receipts
        }
    except Exception as e:
        print(f"❌ Failed to fetch receipts for block {block_number}: {e}")
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

                    # Get historical price once per (token, date) or defer
                    coingecko_id = ADDRESS_TO_ID_MAP.get(token_contract)
                    if DEFER_PRICES:
                        enqueue_price_task(token_contract, date_str)
                        price = None
                    else:
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

                    # Record token in registry for later joins/backfills
                    try:
                        upsert_token_registry(token_contract, metadata.get('symbol'), int(metadata.get('decimals', 18)), block_number, int(timestamp))
                    except Exception:
                        pass

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

def upsert_token_registry(token_contract: str, symbol: Optional[str], decimals: Optional[int], block_number: int, timestamp: int):
    """Record token metadata to a registry CSV (append-only; de-dup later in batch jobs)."""
    try:
        rec = [{
            'tokenContract': token_contract,
            'symbol': symbol or '',
            'decimals': decimals if decimals is not None else '',
            'firstSeenBlock': block_number,
            'firstSeenAt': timestamp,
        }]
        tokens_csv = os.path.join(OUTPUT_DIR, 'tokens.csv')
        write_csv(tokens_csv, rec, CSV_COLUMNS_TOKENS)
    except Exception as e:
        print(f"⚠️ Failed to write token registry for {token_contract}: {e}")

def enqueue_price_task(token_contract: str, date_str: str):
    """Queue a (token, date) pair for later price backfill, deduped for the run."""
    key = (token_contract.lower(), date_str)
    if key in PRICE_TASKS_SET:
        return
    PRICE_TASKS_SET.add(key)
    try:
        rec = [{ 'tokenContract': token_contract, 'date': date_str }]
        tasks_csv = os.path.join(OUTPUT_DIR, 'price_tasks.csv')
        write_csv(tasks_csv, rec, CSV_COLUMNS_PRICE_TASKS)
    except Exception as e:
        print(f"⚠️ Failed to enqueue price task for {token_contract} @ {date_str}: {e}")

def _serialize_value(v):
    from decimal import Decimal as _D
    if isinstance(v, _D):
        return str(v)
    return v

def write_csv(file_path: str, records: list, columns: list, mode='a'):
    """Append records to a CSV file with provided columns; create header if new."""
    if not records:
        return
    import csv
    is_new = not os.path.exists(file_path)
    with open(file_path, mode, newline='') as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        if is_new:
            writer.writeheader()
        for r in records:
            row = {c: _serialize_value(r.get(c)) for c in columns}
            writer.writerow(row)

def get_pool_tokens(pool_address: str) -> Optional[Dict[str, str]]:
    """Fetch and cache token0/token1 addresses for a given pool (V2/V3/Velodrome-like)."""
    if pool_address in POOL_TOKEN_CACHE:
        return POOL_TOKEN_CACHE[pool_address]
    try:
        # Try standard Uniswap V2/V3 style token0/token1
        abi_token01 = json.loads('[{"inputs":[],"name":"token0","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"token1","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"}]')
        contract = w3.eth.contract(address=pool_address, abi=abi_token01)
        try:
            t0 = contract.functions.token0().call()
            t1 = contract.functions.token1().call()
            res = { 'token0': Web3.to_checksum_address(t0), 'token1': Web3.to_checksum_address(t1) }
        except Exception:
            # Fallback: Solidly/Aerodrome-style tokens() returning (token0, token1)
            abi_tokens = json.loads('[{"inputs":[],"name":"tokens","outputs":[{"internalType":"address","name":"token0","type":"address"},{"internalType":"address","name":"token1","type":"address"}],"stateMutability":"view","type":"function"}]')
            contract2 = w3.eth.contract(address=pool_address, abi=abi_tokens)
            t0, t1 = contract2.functions.tokens().call()
            res = { 'token0': Web3.to_checksum_address(t0), 'token1': Web3.to_checksum_address(t1) }
        POOL_TOKEN_CACHE[pool_address] = res
        return res
    except Exception:
        POOL_TOKEN_CACHE[pool_address] = None
        return None

def parse_and_enrich_swaps(block_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Parse common DEX swap events (Uniswap V2/Solidly-style and Uniswap V3) and enrich with metadata and USD values."""
    receipts = block_data.get('receipts', [])
    block_number = int(block_data.get('blockNumber'))
    # estimate date for pricing from block timestamp via first tx (fallback to today)
    date_str = datetime.now(timezone.utc).strftime('%d-%m-%Y')
    swaps: List[Dict[str, Any]] = []
    try:
        # Best-effort fetch of block timestamp via eth_getBlockByNumber (fast, single call)
        b = w3.eth.get_block(block_number)
        block_ts = int(b.timestamp)
        date_str = datetime.fromtimestamp(block_ts, timezone.utc).strftime('%d-%m-%Y')
    except Exception:
        pass

    scanned_logs = 0
    v2_matches = 0
    v3_matches = 0
    v2_shortdata = 0
    v3_shortdata = 0
    token_resolution_failures = 0
    metadata_failures = 0
    price_failures = 0
    unexpected_errors = 0
    for receipt in receipts:
        tx_hash = receipt.get('transactionHash')
        logs = receipt.get('logs', [])
        for log in logs:
            scanned_logs += 1
            try:
                topic0 = log.get('topics', [None])[0]
                addr = log.get('address')
                if not addr:
                    continue
                try:
                    pool_addr = Web3.to_checksum_address(addr)
                except Exception:
                    continue
                log_index_hex = log.get('logIndex') or log.get('log_index')
                if log_index_hex is None:
                    continue
                log_index = int(log_index_hex, 16) if isinstance(log_index_hex, str) and log_index_hex.startswith('0x') else int(log_index_hex)

                # Uniswap V2 / Velodrome / Solidly style
                if topic0 == UNIV2_SWAP_TOPIC or topic0 == UNIV2_SWAP_TOPIC_ALT:
                    v2_matches += 1
                    # data encodes: amount0In, amount1In, amount0Out, amount1Out (uint256 x4)
                    data_hex = log.get('data', '0x')
                    raw = bytes.fromhex(data_hex[2:]) if data_hex.startswith('0x') else bytes()
                    if len(raw) >= 32*4:
                        amounts = w3.codec.decode(['uint256','uint256','uint256','uint256'], raw)
                        amount0_in, amount1_in, amount0_out, amount1_out = [Decimal(int(x)) for x in amounts]
                    else:
                        v2_shortdata += 1
                        print(f"    V2 Swap data too short (len={len(raw)} bytes) for pool {pool_addr}; skipping")
                        continue
                    tokens = get_pool_tokens(pool_addr)
                    if not tokens:
                        token_resolution_failures += 1
                        print(f"    V2 swap match but tokens unavailable for pool {pool_addr}; skipping")
                        continue
                    token0 = tokens['token0']; token1 = tokens['token1']
                    # decide direction
                    if amount0_in > 0:
                        token_in, token_out = token0, token1
                        amt_in, amt_out = amount0_in, amount1_out
                    else:
                        token_in, token_out = token1, token0
                        amt_in, amt_out = amount1_in, amount0_out

                    # metadata and normalization (resilient)
                    sym_in = None; sym_out = None
                    dec_in = 18; dec_out = 18
                    try:
                        meta_in = get_token_metadata(token_in) or {}
                        meta_out = get_token_metadata(token_out) or {}
                        dec_in = int(meta_in.get('decimals', 18))
                        dec_out = int(meta_out.get('decimals', 18))
                        sym_in = meta_in.get('symbol')
                        sym_out = meta_out.get('symbol')
                    except Exception as e:
                        metadata_failures += 1
                        print(f"    V2 metadata fetch failed for pool {pool_addr}: {e}")
                    norm_in = amt_in / (Decimal(10) ** dec_in) if amt_in is not None else None
                    norm_out = amt_out / (Decimal(10) ** dec_out) if amt_out is not None else None

                    # Token registry upsert for both sides
                    ts_val = int(b.timestamp) if 'b' in locals() else int(time.time())
                    try:
                        upsert_token_registry(token_in, sym_in, dec_in, block_number, ts_val)
                        upsert_token_registry(token_out, sym_out, dec_out, block_number, ts_val)
                    except Exception:
                        pass

                    # USD valuation (resilient) or deferred pricing
                    usd_in = None; usd_out = None
                    if DEFER_PRICES:
                        try:
                            enqueue_price_task(token_in, date_str)
                            enqueue_price_task(token_out, date_str)
                        except Exception:
                            pass
                    else:
                        try:
                            cg_in = ADDRESS_TO_ID_MAP.get(token_in)
                            cg_out = ADDRESS_TO_ID_MAP.get(token_out)
                            price_in = get_historical_price(cg_in, date_str)
                            price_out = get_historical_price(cg_out, date_str)
                            usd_in = (norm_in * Decimal(str(price_in))) if (norm_in is not None and price_in is not None) else None
                            usd_out = (norm_out * Decimal(str(price_out))) if (norm_out is not None and price_out is not None) else None
                        except Exception as e:
                            price_failures += 1
                            print(f"    V2 pricing fetch failed for pool {pool_addr}: {e}")

                    swaps.append({
                        'transactionHash': tx_hash,
                        'logIndex': log_index,
                        'blockNumber': block_number,
                        'timestamp': int(b.timestamp) if 'b' in locals() else int(time.time()),
                        'chain': CHAIN,
                        'poolContract': pool_addr,
                        'tokenInContract': token_in,
                        'tokenInSymbol': sym_in,
                        'amountIn': norm_in,
                        'usdValueIn': usd_in,
                        'tokenOutContract': token_out,
                        'tokenOutSymbol': sym_out,
                        'amountOut': norm_out,
                        'usdValueOut': usd_out,
                    })

                # Uniswap V3 style
                elif topic0 == UNIV3_SWAP_TOPIC:
                    v3_matches += 1
                    data_hex = log.get('data', '0x')
                    raw = bytes.fromhex(data_hex[2:]) if data_hex.startswith('0x') else bytes()
                    if len(raw) >= 32*5:  # amounts and other fields
                        # Decode full tuple, then use first two values (amount0, amount1)
                        decoded = w3.codec.decode(['int256','int256','uint160','uint128','int24'], raw)
                        amount0 = Decimal(int(decoded[0]))
                        amount1 = Decimal(int(decoded[1]))
                    else:
                        v3_shortdata += 1
                        print(f"    V3 Swap data too short (len={len(raw)} bytes) for pool {pool_addr}; skipping")
                        continue
                    tokens = get_pool_tokens(pool_addr)
                    if not tokens:
                        token_resolution_failures += 1
                        print(f"    V3 swap match but tokens unavailable for pool {pool_addr}; skipping")
                        continue
                    token0 = tokens['token0']; token1 = tokens['token1']
                    # In V3, positive amount means to the pool (in), negative means out from pool
                    if amount0 > 0:
                        token_in, token_out = token0, token1
                        amt_in, amt_out = amount0, -amount1
                    else:
                        token_in, token_out = token1, token0
                        amt_in, amt_out = amount1, -amount0

                    sym_in = None; sym_out = None
                    dec_in = 18; dec_out = 18
                    try:
                        meta_in = get_token_metadata(token_in) or {}
                        meta_out = get_token_metadata(token_out) or {}
                        dec_in = int(meta_in.get('decimals', 18))
                        dec_out = int(meta_out.get('decimals', 18))
                        sym_in = meta_in.get('symbol')
                        sym_out = meta_out.get('symbol')
                    except Exception as e:
                        metadata_failures += 1
                        print(f"    V3 metadata fetch failed for pool {pool_addr}: {e}")
                    norm_in = amt_in / (Decimal(10) ** dec_in) if amt_in is not None else None
                    norm_out = amt_out / (Decimal(10) ** dec_out) if amt_out is not None else None

                    # Token registry upsert for both sides
                    ts_val = int(b.timestamp) if 'b' in locals() else int(time.time())
                    try:
                        upsert_token_registry(token_in, sym_in, dec_in, block_number, ts_val)
                        upsert_token_registry(token_out, sym_out, dec_out, block_number, ts_val)
                    except Exception:
                        pass

                    usd_in = None; usd_out = None
                    if DEFER_PRICES:
                        try:
                            enqueue_price_task(token_in, date_str)
                            enqueue_price_task(token_out, date_str)
                        except Exception:
                            pass
                    else:
                        try:
                            cg_in = ADDRESS_TO_ID_MAP.get(token_in)
                            cg_out = ADDRESS_TO_ID_MAP.get(token_out)
                            price_in = get_historical_price(cg_in, date_str)
                            price_out = get_historical_price(cg_out, date_str)
                            usd_in = (norm_in * Decimal(str(price_in))) if (norm_in is not None and price_in is not None) else None
                            usd_out = (norm_out * Decimal(str(price_out))) if (norm_out is not None and price_out is not None) else None
                        except Exception as e:
                            price_failures += 1
                            print(f"    V3 pricing fetch failed for pool {pool_addr}: {e}")

                    swaps.append({
                        'transactionHash': tx_hash,
                        'logIndex': log_index,
                        'blockNumber': block_number,
                        'timestamp': int(b.timestamp) if 'b' in locals() else int(time.time()),
                        'chain': CHAIN,
                        'poolContract': pool_addr,
                        'tokenInContract': token_in,
                        'tokenInSymbol': sym_in,
                        'amountIn': norm_in,
                        'usdValueIn': usd_in,
                        'tokenOutContract': token_out,
                        'tokenOutSymbol': sym_out,
                        'amountOut': norm_out,
                        'usdValueOut': usd_out,
                    })

            except Exception as e:
                unexpected_errors += 1
                try:
                    topic0_dbg = log.get('topics', [None])[0]
                except Exception:
                    topic0_dbg = None
                print(f"    Unexpected error decoding swap log: pool={addr}, topic0={topic0_dbg}, err={e}")
                continue

    print(
        f"Swaps scan summary for block {block_number}: "
        f"scanned_logs={scanned_logs}, v2_topic_matches={v2_matches}, v3_topic_matches={v3_matches}, "
        f"decoded_swaps={len(swaps)}, v2_shortdata={v2_shortdata}, v3_shortdata={v3_shortdata}, "
        f"token_resolution_failures={token_resolution_failures}, metadata_failures={metadata_failures}, price_failures={price_failures}, unexpected_errors={unexpected_errors}"
    )
    return swaps

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
    db_conn = get_db_connection() if USE_DB else None
    if USE_DB and not db_conn:
        exit()

    try:
        latest_block_number = w3.eth.block_number
        target_latest = max(0, latest_block_number - CONFIRMATIONS)
        print(f"\nLatest block on Base: {latest_block_number}. Target up to: {target_latest} (confirmations: {CONFIRMATIONS})")

        if USE_DB:
            start_block = get_last_processed_block(db_conn, CHAIN)
            if start_block is None:
                start_block = target_latest  # start from tip (no backfill on first run)
            else:
                start_block = start_block + 1
        else:
            start_block = target_latest

        if start_block > target_latest:
            print("No new blocks to process.")
        else:
            for block_number in range(start_block, target_latest + 1):
                block_data = get_block_with_receipts(block_number)
                if not block_data:
                    continue
                enriched_transfers = parse_and_enrich_transfers(block_data)
                if enriched_transfers:
                    insert_transfers(db_conn, block_number, enriched_transfers)
                enriched_swaps = parse_and_enrich_swaps(block_data)
                if enriched_swaps:
                    insert_swaps(db_conn, block_number, enriched_swaps)
                else:
                    print("No swaps decoded for this block.")
                if USE_DB:
                    set_last_processed_block(db_conn, CHAIN, block_number)
                print(f"Processed block {block_number}.")

        print("\nPipeline run complete.")

    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")
    finally:
        if USE_DB and db_conn:
            db_conn.close()
            print("\nDatabase connection closed.")
