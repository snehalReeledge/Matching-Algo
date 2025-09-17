import os
import json
import requests
from concurrent.futures import ThreadPoolExecutor
from config import UPDATE_BANK_TRANSACTIONS_API_URL, BANK_TRANSACTIONS_API_URL

# This is the list of platform transaction IDs that had their dates modified.
# We will unlink the corresponding bank transaction for each of these, except for 117158.
TRANSACTION_IDS_TO_PROCESS = [
    187114, 138999, 183172, 182638, 109979, 163819, 175583, 108068, 173324, 127431,
    168879, 97256, 161822, 165095, 93041, 126568, 121416, 133692, 181819, 115886,
    133683, 175588, 94887, 128150, 163905, 157332, 127406, 126723, 115760, 163305,
    151518, 150622, 181856, 133690, 195943, 184562, 173463, 161571, 160733, 189445,
    188828, 154380, 130279, 160044, 194158, 153866, 146328, 189551, 130500, 137107,
    187982, 123017, 143956, 193880, 152108, 158910, 140543, 133762, 168656, 152854,
    161109, 111830, 196207, 107132, 194107, 91482, 184806, 83824, 154378,
    81600, 164811, 71261, 71069, 156982, 155606, 139750, 194204, 128927, 152017,
    128153, 193828, 127703, 146778, 74068, 186423, 127436, 180311, 127601, 123319,
    195427, 167796, 120072, 116172, 188493, 166855, 115303, 115614, 183701, 163584,
    153707, 115469, 182892, 103015, 152140, 133887, 114347, 181130, 98952, 151264,
    130504, 113823, 98373, 148867, 113355, 91686, 147980, 113131, 90574, 145870,
    112365, 76550, 139013, 111611, 163185, 134963, 191588, 138223, 134299, 156412,
    147333, 123957, 123959, 123019, 173781, 188579
]

def unlink_bank_transaction(bank_transaction_id):
    """
    Updates a bank transaction to remove its link to a platform transaction.
    """
    url = UPDATE_BANK_TRANSACTIONS_API_URL.format(transaction_id=bank_transaction_id)
    payload = {
        "transaction_link": None
    }
    try:
        response = requests.patch(url, json=payload)
        response.raise_for_status()
        print(f"Successfully unlinked bank transaction {bank_transaction_id}.")
    except requests.exceptions.RequestException as e:
        print(f"Failed to unlink bank transaction {bank_transaction_id}: {e}")

def build_transaction_to_player_map():
    """
    Scans the debug directory to find which player each platform transaction belongs to.
    """
    print("Building map from transaction ID to player ID...")
    transaction_map = {}
    debug_dir = 'debug'
    
    player_dirs = [d for d in os.listdir(debug_dir) if d.startswith('player_') and os.path.isdir(os.path.join(debug_dir, d))]

    for player_dir in player_dirs:
        player_id = player_dir.split('_')[1]
        matched_pairs_file = os.path.join(debug_dir, player_dir, 'matched_pairs.json')
        
        if os.path.exists(matched_pairs_file):
            try:
                with open(matched_pairs_file, 'r') as f:
                    data = json.load(f)
                for match in data.get('received_matches', []):
                    if pt_id := match.get('platform_transaction_id'):
                        transaction_map[pt_id] = player_id
                for match in data.get('returned_matches', []):
                    if pt_id := match.get('platform_transaction_id'):
                        transaction_map[pt_id] = player_id
            except (json.JSONDecodeError, IOError):
                pass
    
    print(f"Map built successfully. Found associations for {len(transaction_map)} transactions.")
    return transaction_map

def fetch_bank_transactions(player_id):
    """
    Fetches all bank transactions for a given player.
    """
    try:
        response = requests.get(BANK_TRANSACTIONS_API_URL, params={'player_id': player_id}, timeout=60)
        response.raise_for_status()
        return response.json().get('bankTransactions', [])
    except requests.exceptions.RequestException as e:
        print(f"Error fetching bank transactions for player {player_id}: {e}")
        return []

def get_bank_transaction_id(player_id, platform_id_to_find, all_bank_tx):
    """
    Finds the linked bank transaction ID from the debug files for a given platform transaction.
    """
    internal_bank_id = None
    matched_pairs_file = os.path.join('debug', f'player_{player_id}_investigation', 'matched_pairs.json')
    if os.path.exists(matched_pairs_file):
        try:
            with open(matched_pairs_file, 'r') as f:
                data = json.load(f)
            for match_type in ['received_matches', 'returned_matches']:
                for match in data.get(match_type, []):
                    if match.get('platform_transaction_id') == platform_id_to_find:
                        internal_bank_id = match.get('bank_transaction_id')
                        break
                if internal_bank_id:
                    break
        except (json.JSONDecodeError, IOError):
            pass

    if internal_bank_id:
        for bt in all_bank_tx:
            if bt.get('id') == internal_bank_id:
                return bt.get('transaction_id')
            
    return None

def process_single_transaction(platform_id, tx_to_player_map):
    """
    Orchestrates finding and unlinking the bank transaction for a single platform transaction.
    """
    if not (player_id := tx_to_player_map.get(platform_id)):
        print(f"Warning: No player found for PT ID {platform_id}.")
        return

    # Fetch all bank transactions for the player once
    all_bank_tx = fetch_bank_transactions(player_id)
    if not all_bank_tx:
        print(f"Warning: Could not fetch bank transactions for player {player_id}. Skipping.")
        return

    if bank_tx_id := get_bank_transaction_id(player_id, platform_id, all_bank_tx):
        print(f"Found bank transaction {bank_tx_id} for platform transaction {platform_id}. Unlinking...")
        unlink_bank_transaction(bank_tx_id)
    else:
        print(f"Warning: Could not find linked bank transaction for PT ID {platform_id}. Skipping.")

def main():
    """
    Main function to orchestrate the entire unlinking process.
    """
    tx_to_player_map = build_transaction_to_player_map()
    
    ids_to_process = [tx_id for tx_id in TRANSACTION_IDS_TO_PROCESS if tx_id != 117158 and tx_id in tx_to_player_map]
    
    print(f"\nFound {len(ids_to_process)} transactions to process.")
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(process_single_transaction, tx_id, tx_to_player_map) for tx_id in ids_to_process]
        for future in futures:
            future.result()
            
    print("\nUnlinking process completed.")

if __name__ == "__main__":
    main()
