#!/usr/bin/env python3
"""
Analyzes player bank transactions to find "orphan" inbound transfers from PayPal
that are not linked to any platform transaction. It generates a report of
proposed actions for creating the missing platform transactions.

This script is for reporting and does NOT modify any data.
"""

import json
import os
import re
import argparse
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

from config import (
    PLAYERS_API_URL,
    BANK_TRANSACTIONS_API_URL,
    AI_USER_ID,
    USER_ACCOUNTS_API_URL
)
from create_platform_transaction import create_platform_transaction
from run_transfer_player_matcher import run_matcher_for_player

# --- Constants ---
# Keywords to identify inbound transfers from PayPal on a betting bank account
BETTING_BANK_TRANSFER_KEYWORDS = [
    "^paypal transfer$",
    "^paypal transfer.*add to balance",
    "add to balance paypal transfer$",
    "paypal inst xfer",
    "inst xfer paypal",
    "paypal acctverify",
    "paypal retry pymt",
    "money transfer authorized",
    "paypal reversal",
    "paypal recovery",
    "recovery paypal",
    "paypal des",
    "pmnt sent",
    "visa.*paypal",
    "paypal.*visa direct",
    "^paypal$"
]

# --- Helper Functions ---

def get_players_by_stage(stage: str) -> list:
    """Fetches a list of player objects for a given stage, excluding internal accounts."""
    print(f"Fetching '{stage}' players (excluding @reeledge.com)...")
    try:
        response = requests.get(PLAYERS_API_URL, timeout=60)
        response.raise_for_status()
        
        all_players = response.json()
        stage_players = [p for p in all_players if p.get('player_stage') == stage]
        filtered_players = [p for p in stage_players if '@reeledge.com' not in p.get('email', '').lower()]
        
        print(f"Found {len(filtered_players)} '{stage}' players to analyze.")
        return filtered_players
    except requests.RequestException as e:
        print(f"Error fetching players: {e}")
        return []

def get_bank_transactions(player_id: int) -> list:
    """Fetches all bank transactions for a given player."""
    try:
        response = requests.get(BANK_TRANSACTIONS_API_URL, params={'player_id': player_id})
        response.raise_for_status()
        return response.json().get('bankTransactions', [])
    except requests.RequestException as e:
        print(f"Error fetching bank transactions for player {player_id}: {e}")
        return []

def get_user_accounts(player_id: int) -> list:
    """Fetches all user accounts for a given player."""
    try:
        response = requests.get(USER_ACCOUNTS_API_URL, params={'user_id': player_id})
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching user accounts for player {player_id}: {e}")
        return []

def find_orphan_transactions(player_id: int):
    """
    Finds orphan bank transactions for a single player and returns a list of proposed actions.
    """
    print(f"--- Processing Player ID: {player_id} ---")
    proposed_actions = []
    
    bank_transactions = get_bank_transactions(player_id)
    user_accounts = get_user_accounts(player_id)
    
    if not bank_transactions or not user_accounts:
        print(f"  -> No bank transactions or user accounts found. Skipping.")
        return []

    # Find the default Betting PayPal account
    betting_paypal_account = next((acc for acc in user_accounts if acc.get('Account_Type', '').lower() == 'betting paypal account' and acc.get('isDefault')), None)

    if not betting_paypal_account:
        print(f"  -> WARNING: Could not find a default Betting PayPal account for player {player_id}. Skipping.")
        return []
    
    to_account_id = betting_paypal_account.get('id')

    # Define the date threshold (15 days ago)
    fifteen_days_ago = datetime.now() - timedelta(days=15)

    # Filter for orphan transactions
    for bt in bank_transactions:
        is_orphan = not bt.get('linked_transaction')
        is_significant_transfer = bt.get('amount', 0) > 2
        
        try:
            transaction_date = datetime.strptime(bt.get('date', ''), '%Y-%m-%d')
            is_older_than_15_days = transaction_date < fifteen_days_ago
        except (ValueError, TypeError):
            continue # Skip if date is invalid

        # Find the source account and check if it's a betting bank account
        source_account = next((acc for acc in user_accounts if acc.get('bankaccount_id') == bt.get('bankaccount_id')), None)
        
        if not source_account or source_account.get('Account_Type', '').lower() != 'betting bank account':
            continue

        description = bt.get('name', '').lower()
        matches_keyword = any(re.search(k, description) for k in BETTING_BANK_TRANSFER_KEYWORDS)

        if is_orphan and is_significant_transfer and is_older_than_15_days and matches_keyword:
            from_account = source_account # We've already found the correct account
            
            if not from_account:
                print(f"  -> WARNING: Could not find a matching 'from' account for BT ID {bt.get('id')}. Skipping.")
                continue

            from_account_id = from_account.get('id')
            
            print(f"  -> Found orphan bank transaction: ID {bt.get('id')}, Amount: {bt.get('amount')}")
            
            proposed_actions.append({
                "action": "CREATE_AND_LINK_TRANSFER",
                "player_id": player_id,
                "orphan_bank_transaction": bt,
                "proposed_platform_transaction": {
                    "Transaction_Type": "transfer",
                    "From_Account": from_account_id,
                    "To_Account": to_account_id,
                    "Amount": bt.get('amount'),
                    "Date": bt.get('date'),
                    "Status": "Completed",
                    "Added_By": AI_USER_ID,
                    "User_ID": player_id,
                    "created_at": int(datetime.now().timestamp() * 1000)
                }
            })
            
    return proposed_actions

# --- Main Execution ---

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Find orphan bank transactions and generate a report for one or more player stages."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--player_stages", nargs='+', type=str, help="One or more player stages to process (e.g., 'Batch 1' 'Batch 2').")
    group.add_argument("--player_id", type=int, help="A specific player ID to process.")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Execute the proposed actions instead of just generating a report."
    )
    args = parser.parse_args()

    all_proposed_actions = []

    if args.player_id:
        all_proposed_actions = find_orphan_transactions(args.player_id)
    
    elif args.player_stages:
        players_to_process = []
        for stage in args.player_stages:
            players_to_process.extend(get_players_by_stage(stage))

        if players_to_process:
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = [executor.submit(find_orphan_transactions, player.get('id')) for player in players_to_process]
                for future in futures:
                    all_proposed_actions.extend(future.result())
        else:
            print("No players to process for the specified stages. Exiting.")
            # We should exit here if there are no players to process
            exit()

    if args.execute:
        if all_proposed_actions:
            print(f"\n--- EXECUTING {len(all_proposed_actions)} PROPOSED ACTIONS ---")
            
            # Keep track of which players have new transactions created
            player_ids_with_new_transactions = set()
            
            for action in all_proposed_actions:
                if action.get('action') == 'CREATE_AND_LINK_TRANSFER':
                    proposed_pt = action.get('proposed_platform_transaction', {})
                    orphan_bt = action.get('orphan_bank_transaction', {})
                    
                    if not proposed_pt or not orphan_bt:
                        print("  -> ERROR: Incomplete action data. Skipping.")
                        continue

                    print(f"  -> Creating transaction for player {proposed_pt.get('User_ID')} with amount {proposed_pt.get('Amount')}...")
                    
                    # 1. Create the new platform transaction
                    new_platform_transaction = create_platform_transaction({}, proposed_pt)
                    
                    if new_platform_transaction and new_platform_transaction.get('id'):
                        new_pt_id = new_platform_transaction.get('id')
                        print(f"    - Successfully created Platform Transaction ID: {new_pt_id}")
                        
                        # Add the player_id to our set to run the matcher later
                        player_id = proposed_pt.get('User_ID')
                        if player_id:
                            player_ids_with_new_transactions.add(player_id)
                    else:
                        print("    - FAILED to create platform transaction.")
            print("\n--- EXECUTION COMPLETE ---")
            
            # After creating transactions, run the transfer matcher for the affected players
            if player_ids_with_new_transactions:
                print(f"\n--- Running transfer matcher for {len(player_ids_with_new_transactions)} player(s) with new transactions... ---")
                for player_id in player_ids_with_new_transactions:
                    run_matcher_for_player(player_id)
                print("\n--- Transfer matching complete. ---")
                
        else:
            print("\nNo actions to execute.")
        
    else:
        # Save the report if not executing
        if all_proposed_actions:
            output_dir = "debug_create_transfer"
            os.makedirs(output_dir, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            if args.player_id:
                base_filename = f"proposed_missing_transfers_player_{args.player_id}_{timestamp}.json"
            else:
                stages_str = "_".join(s.replace(' ', '_') for s in args.player_stages)
                base_filename = f"proposed_missing_transfers_{stages_str}_{timestamp}.json"
            
            report_filepath = os.path.join(output_dir, base_filename)
            
            with open(report_filepath, 'w') as f:
                json.dump(all_proposed_actions, f, indent=4)
                
            print(f"\n✅ Report complete. Found {len(all_proposed_actions)} proposed actions.")
            print(f"Results saved to: {report_filepath}")
        else:
            print("\n✅ Analysis complete. No orphan transactions found matching the criteria.")
