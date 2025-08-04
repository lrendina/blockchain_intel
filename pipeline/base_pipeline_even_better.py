import os
import json
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware
from dotenv import load_dotenv
import requests
from typing import List, Dict, Any, Optional
from datetime import datetime
import time

# --- 1. SETUP & CONNECTIONS ---

load_dotenv()
QUICKNODE_URL = os.getenv("QUICKNODE_BASE_URL")
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY")

if not QUICKNODE_URL or not ETHERSCAN_API_KEY:
    raise Exception("QUICKNODE_BASE_URL and ETHERSCAN_API_KEY must be set in the .env file.")

w3 = Web3(Web3.HTTPProvider(QUICKNODE_URL))
w3.middleware_onion.inject(ExtraDataToPOAMiddleware(), layer=0)

if not w3.is_connected():
    raise Exception("Failed to connect to the Base network via your RPC provider.")

print(f"✅ Successfully connected to Base network (Chain ID: {w3.eth.chain_id})")

# --- CONSTANTS & CACHES ---

TRANSFER_EVENT_TOPIC = w3.keccak(text="Transfer(address,address,uint256)").to_0x_hex()
ERC20_ABI = json.loads('[{"constant":true,"inputs":[],"name":"name","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"}]')

# Caches to minimize external API calls and file reads
TOKEN_METADATA_CACHE: Dict[str, Optional[Dict[str, Any]]] = {}
COINGECKO_ID_CACHE: Dict[str, Optional[str]] = {}
PRICE_CACHE: Dict[str, Optional[float]] = {}
COINGECKO_TOKEN_LIST: List[Dict[str, str]] = []

# --- 2. NEW: DATA LOADING ---

def load_coingecko_list(filepath="../coin_list.json"):
    """Loads the master list of tokens from CoinGecko."""
    global COINGECKO_TOKEN_LIST
    try:
        with open(filepath, 'r') as f:
            COINGECKO_TOKEN_LIST = json.load(f)
        print(f"✅ Successfully loaded {len(COINGECKO_TOKEN_LIST)} tokens from CoinGecko list.")
    except FileNotFoundError:
        print(f"⚠️ Warning: '{filepath}' not found. Price enrichment will be disabled.")
    except json.JSONDecodeError:
        print(f"❌ Error: Could not decode JSON from '{filepath}'.")

# --- 3. CORE DATA FETCHING LOGIC ---

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
        headers = {
            'Content-Type': 'application/json'
        }
        response = requests.request("POST", QUICKNODE_URL, headers=headers, data=payload)
        response.raise_for_status()
        data = response.json()
        if "error" in data:
            print(f"❌ RPC Error: {data['error']['message']}")
            return None
        block_info = w3.eth.get_block(block_number)
        result = {"receipts": data.get("result", []), "timestamp": block_info['timestamp']}
        print(f"✅ Found {len(result['receipts'])} receipts for block {block_number}.")
        return result
    except Exception as e:
        print(f"❌ Error in get_block_with_receipts: {e}")
        return None

# --- 4. DATA PARSING & ENRICHMENT ---

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

                    coingecko_id = find_coingecko_id(token_contract, metadata)
                    price = get_historical_price(coingecko_id, date_str) if coingecko_id else None

                    from_address = Web3.to_checksum_address('0x' + log['topics'][1][-40:])
                    to_address = Web3.to_checksum_address('0x' + log['topics'][2][-40:])
                    
                    log_data = log.get('data', '0x')
                    raw_value = 0 if log_data == '0x' else int(log_data, 16)
                    actual_value = raw_value / (10 ** metadata['decimals'])
                    usd_value = actual_value * price if price is not None else None

                    enriched_transfers.append({
                        "blockNumber": int(receipt['blockNumber'], 16),
                        "transactionHash": receipt['transactionHash'],
                        "tokenContract": token_contract,
                        "tokenName": metadata['name'],
                        "tokenSymbol": metadata['symbol'],
                        "fromAddress": from_address,
                        "toAddress": to_address,
                        "value": f"{actual_value:.8f}",
                        "usdValue": f"{usd_value:.2f}" if usd_value is not None else "N/A"
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

def find_coingecko_id(token_address: str, metadata: Dict[str, Any]) -> Optional[str]:
    """Finds the CoinGecko ID for a token using its symbol and name, with caching."""
    if token_address in COINGECKO_ID_CACHE: return COINGECKO_ID_CACHE[token_address]
    if not COINGECKO_TOKEN_LIST: return None

    onchain_symbol = metadata.get('symbol', '').lower()
    onchain_name = metadata.get('name', '').lower()
    
    # Find all potential matches by symbol
    candidates = [token for token in COINGECKO_TOKEN_LIST if token.get('symbol', '').lower() == onchain_symbol]
    
    found_id = None
    if len(candidates) == 1:
        found_id = candidates[0].get('id')
    elif len(candidates) > 1:
        # If multiple tokens share a symbol, use the name to disambiguate
        for candidate in candidates:
            if candidate.get('name', '').lower() == onchain_name:
                found_id = candidate.get('id')
                break
    
    COINGECKO_ID_CACHE[token_address] = found_id
    return found_id

def get_historical_price(coingecko_id: Optional[str], date_str: str) -> Optional[float]:
    """Gets historical price for a given CoinGecko ID on a specific date."""
    if not coingecko_id: return None

    cache_key = f"{coingecko_id}-{date_str}"
    if cache_key in PRICE_CACHE: return PRICE_CACHE[cache_key]

    try:
        print(f"    Fetching price for {coingecko_id} on {date_str}...")
        url = f"https://api.coingecko.com/api/v3/coins/{coingecko_id}/history?date={date_str}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        price = data.get('market_data', {}).get('current_price', {}).get('usd')
        PRICE_CACHE[cache_key] = price
        time.sleep(1.5) #  CoinGecko's free tier rate limit
        return price
    except Exception:
        PRICE_CACHE[cache_key] = None
        return None

# --- 5. MAIN EXECUTION ---

if __name__ == "__main__":
    # Load the master token list at startup
    load_coingecko_list()
    
    try:
        latest_block_number = w3.eth.block_number
        print(f"Latest block on Base: {latest_block_number}")

        block_data = get_block_with_receipts(latest_block_number)

        if block_data:
            enriched_transfers = parse_and_enrich_transfers(block_data)
            
            if enriched_transfers:
                output_filename = f"final_enriched_transfers_block_{latest_block_number}.json"
                with open(output_filename, 'w') as f:
                    json.dump(enriched_transfers, f, indent=2)
                
                print(f"\nSuccessfully saved fully enriched data to '{output_filename}'")

    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")
