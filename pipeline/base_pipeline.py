import os
import json
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware
from dotenv import load_dotenv
import requests
from typing import List, Dict, Any

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

# The event signature for an ERC-20 Transfer is: Transfer(address,address,uint256)
# We need its Keccak-256 hash to identify these events in the transaction logs.
TRANSFER_EVENT_TOPIC = w3.keccak(text="Transfer(address,address,uint256)").to_0x_hex()
print(f"ERC-20 Transfer Event Topic: {TRANSFER_EVENT_TOPIC}")

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

def parse_transfers_from_receipts(receipts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Parses a list of transaction receipts to find and decode ERC-20 Transfer events.
    """
    print(f"\nParsing {len(receipts)} receipts for ERC-20 transfers...")
    decoded_transfers = []

    for receipt in receipts:
        # Each receipt has a 'logs' array
        for log in receipt.get('logs', []):
            # A log's 'topics' array contains indexed event parameters.
            # For an ERC-20 Transfer, topic[0] is the event signature.
            if log['topics'] and log['topics'][0] == TRANSFER_EVENT_TOPIC:
                try:
                    # The 'from' and 'to' addresses are the 2nd and 3rd topics.
                    # They are 32 bytes long, so we slice the last 20 bytes for the address.
                    from_address = Web3.to_checksum_address('0x' + log['topics'][1][-40:])
                    to_address = Web3.to_checksum_address('0x' + log['topics'][2][-40:])
                    
                    # The 'value' is in the 'data' field, which is not indexed.
                    # It's a hex string representing a uint256.
                    value = int(log['data'], 16)
                    
                    # The address of the token contract that emitted this event
                    token_contract = Web3.to_checksum_address(log['address'])

                    transfer_data = {
                        "blockNumber": int(receipt['blockNumber'], 16),
                        "transactionHash": receipt['transactionHash'],
                        "logIndex": int(log['logIndex'], 16),
                        "tokenContract": token_contract,
                        "fromAddress": from_address,
                        "toAddress": to_address,
                        "value": str(value) # Store as string to handle large numbers
                    }
                    decoded_transfers.append(transfer_data)
                except Exception as e:
                    # This can happen with non-standard ERC-20 contracts
                    print(f"⚠️ Could not decode a potential transfer log. Error: {e}")

    print(f"✅ Decoded {len(decoded_transfers)} transfer events.")
    return decoded_transfers


# --- MAIN EXECUTION ---

if __name__ == "__main__":
    try:

        latest_block_number = w3.eth.block_number
        print(f"Latest block on Base: {latest_block_number}")

        # Fetch receipts for the latest block
        receipts = get_receipts_for_block(latest_block_number)

        if receipts:
            # Parse raw data into structured transfers
            transfers = parse_transfers_from_receipts(receipts)
            
            if transfers:
                output_filename = f"transfers_block_{latest_block_number}.json"
            
                with open(output_filename, 'w') as f:
                    json.dump(transfers, f, indent=2)
            
                print(f"\nSuccessfully saved structured transfer data to '{output_filename}'")

    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")