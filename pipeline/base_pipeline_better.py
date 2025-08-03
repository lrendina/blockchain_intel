import os
import json
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware
from dotenv import load_dotenv
import requests
from typing import List, Dict, Any, Optional
from datetime import datetime
import time

load_dotenv()
QUICKNODE_URL = os.getenv("QUICKNODE_BASE_URL")
BASESCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY")

if not QUICKNODE_URL or not BASESCAN_API_KEY:
    raise Exception("QUICKNODE_BASE_URL and BASESCAN_API_KEY must be set in the .env file.")

# Initialize a Web3 instance with your RPC provider
# Note: We will add the PoA middleware needed for Base in the next step
w3 = Web3(Web3.HTTPProvider(QUICKNODE_URL))

w3.middleware_onion.inject(ExtraDataToPOAMiddleware(), layer=0)

if not w3.is_connected():
    raise Exception("Failed to connect to the Base network via your RPC provider.")

print(f"✅ Successfully connected to Base network (Chain ID: {w3.eth.chain_id})")

# Constants and Cache

TRANSFER_EVENT_TOPIC = w3.keccak(text="Transfer(address,address,uint256)").to_0x_hex()
ERC20_ABI = json.loads('[{"constant":true,"inputs":[],"name":"name","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"}]')
COINGECKO_ASSET_PLATFORM_ID = 'base'
# Caches to minimize api calls
TOKEN_METADATA_CACHE: Dict[str, Dict[str, Any]] = {}
PRICE_CACHE: Dict[str, Optional[float]] = {}

TOKEN_ID_MAP = {
    "0x4200000000000000000000000000000000000006": "ethereum",  # Wrapped Ether (WETH)
    "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913": "usd-coin",  # USD Coin (USDC)
    "0x50c5725949a6f092e06c420be4227906c62a92a5": "dai",       # Dai Stablecoin (DAI)
    "0x940181a94a35a4569e4529a3cdfb74e38fd98631": "tether",    # Tether (USDT) - Bridged
    "0x4ed4e862860bed51a9570b96d89af5e1b0efefed": "degen-base" # Degen
}

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
        
        # We need the block timestamp later, so let's get it now.
        block_info = w3.eth.get_block(block_number)
        result = {"receipts": data.get("result", []), "timestamp": block_info['timestamp']}
        
        print(f"✅ Found {len(result['receipts'])} receipts for block {block_number}.")
        return result
    except Exception as e:
        print(f"❌ Error in get_block_with_receipts: {e}")
        return None
    
def get_token_metadata(token_address: str) -> Optional[Dict[str, Any]]:
    """
    Fetches ERC-20 token metadata (name, symbol, decimals) using a cache.
    """
    if token_address in TOKEN_METADATA_CACHE:
        return TOKEN_METADATA_CACHE[token_address]
    
    try:
        contract = w3.eth.contract(address=token_address, abi=ERC20_ABI)
        
        metadata = {
            "name": contract.functions.name().call(), 
            "symbol": contract.functions.symbol().call(), 
            "decimals": contract.functions.decimals().call()
            }
        TOKEN_METADATA_CACHE[token_address] = metadata
        return metadata
    except Exception:
        # Some contracts might not be valid ERC-20s or may be proxies.
        TOKEN_METADATA_CACHE[token_address] = None
        return None
    
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

                    from_address = Web3.to_checksum_address('0x' + log['topics'][1][-40:])
                    to_address = Web3.to_checksum_address('0x' + log['topics'][2][-40:])
                    
                    log_data = log.get('data', '0x')
                    raw_value = 0 if log_data == '0x' else int(log_data, 16)
                    actual_value = raw_value / (10 ** metadata['decimals'])
                    
                    # Get USD price
                    price = get_historical_price(token_contract, date_str)
                    usd_value = actual_value * price if price is not None else None

                    enriched_transfers.append({
                        "blockNumber": int(receipt['blockNumber'], 16),
                        "transactionHash": receipt['transactionHash'],
                        "logIndex": int(log['logIndex'], 16),
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

def get_historical_price(token_address: str, date_str: str) -> Optional[float]:
    """Gets historical price for a token on a specific date from CoinGecko, using a cache."""
    coingecko_id = TOKEN_ID_MAP.get(token_address.lower())
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
        time.sleep(1.5) # Respect CoinGecko's free tier rate limit
        return price
    except Exception as e:
        print(f"    ⚠️ Could not fetch price for {coingecko_id}. Error: {e}")
        PRICE_CACHE[cache_key] = None
        return None
    
if __name__ == "__main__":
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
                print("This data is now ready for analysis and storage in a database.")

    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")