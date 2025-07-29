import os
import json
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware
from dotenv import load_dotenv
import requests

# --- 1. SETUP & CONNECTIONS ---

# Load environment variables from .env file
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

# --- 2. CORE DATA FETCHING LOGIC ---

def get_receipts_for_block(block_number):
    """
    Fetches all transaction receipts for a given block number.
    This uses a custom RPC method often available on enhanced nodes like QuickNode.
    
    Args:
        block_number (int): The block number to fetch receipts for.

    Returns:
        list: A list of transaction receipt objects, or None if an error occurs.
    """
    print(f"\nAttempting to fetch receipts for block: {block_number}...")
    try:
        # This is a non-standard, but common, JSON-RPC method.
        # The exact name can vary ('eth_getBlockReceipts', 'alchemy_getTransactionReceipts', etc.)
        # We'll use a generic payload structure.
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

        #response = requests.post(QUICKNODE_URL, json=payload)
        response.raise_for_status() # Raise an exception for bad status codes
        
        data = response.json()
        
        if "error" in data:
            print(f"❌ RPC Error: {data['error']['message']}")
            return None
            
        raw = data.get("result", [])
        if isinstance(raw, dict) and "receipts" in raw:
            receipts = raw["receipts"]
        elif isinstance(raw, list):
            receipts = raw
        else:
            receipts = []
        
        print(f"✅ Found {len(receipts)} receipts for block {block_number}.")
        return receipts

    except requests.exceptions.RequestException as e:
        print(f"❌ Network error while fetching receipts: {e}")
        return None
    except json.JSONDecodeError:
        print("❌ Failed to decode JSON from the response.")
        return None

# --- 3. MAIN EXECUTION ---

if __name__ == "__main__":
    try:
        # Get the latest block number from the chain
        latest_block_number = w3.eth.block_number
        print(f"Latest block on Base: {latest_block_number}")

        # Fetch receipts for the latest block
        receipts = get_receipts_for_block(latest_block_number)

        if receipts:
            # For now, we'll just save the raw receipts to a file to inspect them.
            # In the next steps, we will parse this data.
            output_filename = f"receipts_block_{latest_block_number}.json"
            
            with open(output_filename, 'w') as f:
                json.dump(receipts, f, indent=2)
            
            print(f"\nSuccessfully saved raw receipt data to '{output_filename}'")
            print("Next step: Parse these receipts to extract token transfers and swaps.")

    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")

