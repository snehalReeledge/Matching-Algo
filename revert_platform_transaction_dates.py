import os
import json
from concurrent.futures import ThreadPoolExecutor
from update_platform_transaction import update_platform_transaction_date

# This is the list of platform transaction IDs that had their dates modified
# in the previous run, based on the logs.
TRANSACTION_IDS_TO_REVERT = [
    187114, 138999, 183172, 182638, 109979, 163819, 175583, 108068, 173324, 127431,
    168879, 97256, 161822, 165095, 93041, 126568, 121416, 133692, 181819, 115886,
    133683, 175588, 94887, 128150, 163905, 157332, 127406, 126723, 115760, 163305,
    151518, 150622, 181856, 133690, 195943, 184562, 173463, 161571, 160733, 189445,
    188828, 154380, 130279, 160044, 194158, 153866, 146328, 189551, 130500, 137107,
    187982, 123017, 143956, 193880, 152108, 158910, 140543, 133762, 168656, 152854,
    161109, 117158, 111830, 196207, 107132, 194107, 91482, 184806, 83824, 154378,
    81600, 164811, 71261, 71069, 156982, 155606, 139750, 194204, 128927, 152017,
    128153, 193828, 127703, 146778, 74068, 186423, 127436, 180311, 127601, 123319,
    195427, 167796, 120072, 116172, 188493, 166855, 115303, 115614, 183701, 163584,
    153707, 115469, 182892, 103015, 152140, 133887, 114347, 181130, 98952, 151264,
    130504, 113823, 98373, 148867, 113355, 91686, 147980, 113131, 90574, 145870,
    112365, 76550, 139013, 111611, 163185, 134963, 191588, 138223, 134299, 156412,
    147333, 123957, 123959, 123019, 173781, 188579
]

def build_transaction_to_player_map():
    """
    Scans the debug directory to find which player each platform transaction belongs to.
    This is necessary because we need to know which player's dump file to read.
    """
    print("Building map from transaction ID to player ID...")
    transaction_map = {}
    debug_dir = 'debug'
    
    # List all player investigation directories
    player_dirs = [d for d in os.listdir(debug_dir) if d.startswith('player_') and os.path.isdir(os.path.join(debug_dir, d))]

    for player_dir in player_dirs:
        player_id = player_dir.split('_')[1]
        matched_pairs_file = os.path.join(debug_dir, player_dir, 'matched_pairs.json')
        
        if os.path.exists(matched_pairs_file):
            try:
                with open(matched_pairs_file, 'r') as f:
                    data = json.load(f)
                    
                for match in data.get('received_matches', []):
                    pt_id = match.get('platform_transaction_id')
                    if pt_id:
                        transaction_map[pt_id] = player_id
                        
                for match in data.get('returned_matches', []):
                    pt_id = match.get('platform_transaction_id')
                    if pt_id:
                        transaction_map[pt_id] = player_id
            except (json.JSONDecodeError, IOError) as e:
                print(f"Warning: Could not read or parse {matched_pairs_file}. Error: {e}")
    
    print(f"Map built successfully. Found associations for {len(transaction_map)} transactions.")
    return transaction_map

def get_original_date_from_dump(player_id, transaction_id):
    """
    Reads the platform_transactions_dump.json for a specific player to find the original date
    of a given transaction.
    """
    dump_file = os.path.join('debug', f'player_{player_id}_investigation', 'platform_transactions_dump.json')
    
    if not os.path.exists(dump_file):
        return None
        
    try:
        with open(dump_file, 'r') as f:
            transactions = json.load(f)
        
        for tx in transactions:
            if tx.get('id') == transaction_id:
                return tx.get('Date')
    except (json.JSONDecodeError, IOError) as e:
        print(f"Warning: Could not read or parse dump file {dump_file}. Error: {e}")
        
    return None

def revert_single_transaction(transaction_id, transaction_map):
    """
    Orchestrates the revert process for a single transaction.
    """
    player_id = transaction_map.get(transaction_id)
    if not player_id:
        print(f"Warning: No player found for transaction ID {transaction_id}. Skipping revert.")
        return

    original_date = get_original_date_from_dump(player_id, transaction_id)
    if not original_date:
        print(f"Warning: Could not find original date for transaction ID {transaction_id} in player {player_id}'s dump. Skipping.")
        return
        
    print(f"Reverting date for transaction {transaction_id} to {original_date}...")
    update_platform_transaction_date(transaction_id, original_date)

def main():
    """
    Main function to orchestrate the entire revert process.
    """
    transaction_map = build_transaction_to_player_map()
    
    ids_to_process = [tx_id for tx_id in TRANSACTION_IDS_TO_REVERT if tx_id in transaction_map]
    
    print(f"\nFound {len(ids_to_process)} transactions to revert out of {len(TRANSACTION_IDS_TO_REVERT)} total.")
    
    # Use ThreadPoolExecutor to speed up the API calls
    with ThreadPoolExecutor(max_workers=10) as executor:
        # Submit all revert tasks
        futures = [executor.submit(revert_single_transaction, tx_id, transaction_map) for tx_id in ids_to_process]
        
        # Wait for all futures to complete
        for future in futures:
            future.result()
            
    print("\nDate revert process completed.")

if __name__ == "__main__":
    main()
