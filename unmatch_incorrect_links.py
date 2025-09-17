#!/usr/bin/env python3
"""
Scans for incorrectly matched transactions and reverts them.

An incorrect match is defined as a platform transaction that has a pre-existing
'related_bank_transaction' but was matched by the script to a *different*
bank transaction.

This script will:
1. Scan all player debug directories.
2. For each player, load the matched pairs and the platform transaction dump.
3. Identify platform transactions that were matched incorrectly.
4. For each incorrect match, it will call the update_bank_transaction API
   to remove the link from the bank transaction.
"""

import os
import json
from concurrent.futures import ThreadPoolExecutor
import requests
from config import BANK_TRANSACTIONS_API_URL
from update_bank_transaction import update_bank_transaction

def fetch_bank_transactions(player_id):
    """Fetches all bank transactions for a given player."""
    try:
        response = requests.get(BANK_TRANSACTIONS_API_URL, params={'player_id': player_id}, timeout=60)
        response.raise_for_status()
        return response.json().get('bankTransactions', [])
    except requests.exceptions.RequestException as e:
        print(f"Error fetching bank transactions for player {player_id}: {e}")
        return []

def find_incorrect_matches_for_player(player_dir):
    """
    Finds incorrectly matched transactions for a single player.
    """
    player_id = player_dir.split('_')[1]
    matched_pairs_file = os.path.join(player_dir, 'matched_pairs.json')
    pt_dump_file = os.path.join(player_dir, 'platform_transactions_dump.json')

    if not os.path.exists(matched_pairs_file) or not os.path.exists(pt_dump_file):
        return []

    try:
        with open(matched_pairs_file, 'r') as f:
            matched_pairs = json.load(f)
        with open(pt_dump_file, 'r') as f:
            platform_transactions = json.load(f)
    except (json.JSONDecodeError, IOError):
        return []

    bank_transactions = fetch_bank_transactions(player_id)
    if not bank_transactions:
        print(f"Warning: Could not fetch bank transactions for player {player_id}. Skipping.")
        return []
    
    bt_map_by_db_id = {bt['id']: bt for bt in bank_transactions}
    pt_map = {pt['id']: pt for pt in platform_transactions}
    incorrect_matches = []

    all_matches = matched_pairs.get('received_matches', []) + matched_pairs.get('returned_matches', [])

    for match in all_matches:
        pt_id = match.get('platform_transaction_id')
        matched_bt_db_id = match.get('bank_transaction_id')
        
        platform_transaction = pt_map.get(pt_id)
        if not platform_transaction:
            continue

        related_bt_info = platform_transaction.get('related_bank_transaction')
        if related_bt_info and isinstance(related_bt_info, list) and len(related_bt_info) > 0:
            original_bt_id = related_bt_info[0].get('id')
            if original_bt_id != matched_bt_db_id:
                # This is an incorrect match
                incorrect_bt = bt_map_by_db_id.get(matched_bt_db_id)
                if not incorrect_bt:
                    print(f"Warning: Could not find bank transaction with DB ID {matched_bt_db_id} for player {player_id}")
                    continue
                
                incorrect_bt_transaction_id = incorrect_bt.get('transaction_id')
                if not incorrect_bt_transaction_id:
                    print(f"Warning: Bank transaction with DB ID {matched_bt_db_id} has no 'transaction_id' field.")
                    continue
                
                incorrect_matches.append({
                    "player_id": player_dir.split('_')[1],
                    "platform_transaction_id": pt_id,
                    "original_bank_transaction_id": original_bt_id,
                    "incorrectly_matched_bank_transaction_api_id": incorrect_bt_transaction_id
                })
    
    return incorrect_matches

def unmatch_transaction(bank_transaction_id):
    """
    Calls the update function to unlink a bank transaction.
    """
    if not bank_transaction_id:
        return
    
    print(f"Unlinking bank transaction: {bank_transaction_id}")
    # Pass None to unlink the transaction
    update_bank_transaction(bank_transaction_id, None)

def main():
    """
    Main function to find and unmatch all incorrect links.
    """
    debug_dir = 'debug'
    player_dirs = [os.path.join(debug_dir, d) for d in os.listdir(debug_dir) if d.startswith('player_') and os.path.isdir(os.path.join(debug_dir, d))]

    all_incorrect_matches = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(find_incorrect_matches_for_player, player_dir) for player_dir in player_dirs]
        for future in futures:
            all_incorrect_matches.extend(future.result())

    print(f"Found {len(all_incorrect_matches)} incorrectly matched transactions.")

    if not all_incorrect_matches:
        print("No incorrect matches to revert.")
        return

    # Unlink the bank transactions
    print("\n--- Starting Unlinking Process ---")
    bank_ids_to_unmatch = [match['incorrectly_matched_bank_transaction_api_id'] for match in all_incorrect_matches]
    with ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(unmatch_transaction, bank_ids_to_unmatch)
    print("--- Unlinking complete ---")
        
    print("\nRevert process complete.")

if __name__ == "__main__":
    main()
