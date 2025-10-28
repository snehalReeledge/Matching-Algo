# run_specific_players_create_transfers.py

from create_missing_betting_bank_transfers import find_orphan_transactions
from create_platform_transaction import create_platform_transaction
from run_transfer_player_matcher import run_matcher_for_player
from concurrent.futures import ThreadPoolExecutor

PLAYER_IDS = [
    33176, 32292, 20130, 37896, 32310, 3785, 33063, 18776, 37055, 1461, 
    36641, 15127, 5829, 32643, 32616, 32279, 32274, 36244, 35980, 32313, 
    35166, 1065, 40398, 38559, 33217, 199, 2178, 6112, 26384, 18061, 
    36715, 6222, 29083, 18130, 290, 35804, 35184, 33212, 37123, 36134, 
    40259, 18505, 33252, 36031
]

def process_player(player_id):
    """Finds and creates missing transactions for a single player."""
    all_proposed_actions = find_orphan_transactions(player_id)

    if not all_proposed_actions:
        return None

    print(f"--- EXECUTING {len(all_proposed_actions)} PROPOSED ACTIONS FOR PLAYER {player_id} ---")
    
    player_had_transaction_created = False
    for action in all_proposed_actions:
        if action.get('action') == 'CREATE_AND_LINK_TRANSFER':
            proposed_pt = action.get('proposed_platform_transaction', {})
            
            if not proposed_pt:
                print("  -> ERROR: Incomplete action data. Skipping.")
                continue

            print(f"  -> Creating transaction for player {proposed_pt.get('User_ID')} with amount {proposed_pt.get('Amount')}...")
            
            new_platform_transaction = create_platform_transaction({}, proposed_pt)
            
            if new_platform_transaction and new_platform_transaction.get('id'):
                new_pt_id = new_platform_transaction.get('id')
                print(f"    - Successfully created Platform Transaction ID: {new_pt_id}")
                player_had_transaction_created = True
            else:
                print("    - FAILED to create platform transaction.")
    
    return player_id if player_had_transaction_created else None

if __name__ == "__main__":
    player_ids_with_new_transactions = set()

    with ThreadPoolExecutor(max_workers=5) as executor:
        results = executor.map(process_player, PLAYER_IDS)
        for player_id in results:
            if player_id:
                player_ids_with_new_transactions.add(player_id)
    
    print("\n--- TRANSACTION CREATION COMPLETE ---")

    if player_ids_with_new_transactions:
        print(f"\n--- Running transfer matcher for {len(player_ids_with_new_transactions)} player(s) with new transactions... ---")
        for player_id in player_ids_with_new_transactions:
            run_matcher_for_player(player_id)
        print("\n--- Transfer matching complete. ---")
    else:
        print("\n--- No new transactions were created. ---")
