#!/usr/bin/env python3
"""
Runner script for the SimplifiedPayPalMatcher.
This script fetches the necessary data, runs the simplified matching logic,
and executes the resulting matches according to the defined process for simple
and three-way matches.
"""

import json
import os
from datetime import datetime
import time
from concurrent.futures import ThreadPoolExecutor
import requests

from simplified_paypal_matcher import (
    SimplifiedPayPalMatcher,
    get_all_platform_transactions,
    get_all_bank_transactions,
    get_scraped_transactions,
    SimpleMatch,
    ThreeWayMatch
)
from config import (
    UPDATE_BANK_TRANSACTIONS_API_URL,
    UPDATE_PLATFORM_TRANSACTIONS_API_URL,
    CREATE_PLATFORM_TRANSACTION_API_URL,
    AI_USER_ID,
    TRANSFER_ACCOUNT_ID,
    FEES_ACCOUNT_ID
)

# --- Execution Functions ---

def execute_simple_matches(simple_matches: list[SimpleMatch], dry_run: bool = False):
    """
    Executes simple matches by linking the bank transactions to the platform transaction.
    """
    if not simple_matches:
        print("--- No Simple Matches to Execute ---")
        return

    print(f"--- Executing {len(simple_matches)} Simple Matches ---")
    for match in simple_matches:
        pt_id = match.platform_transaction.get('id')
        for bt in match.bank_transactions:
            bt_id = bt.get('id')
            if dry_run:
                print(f"  [DRY RUN] Would link PT {pt_id} <-> BT {bt_id}")
            else:
                print(f"  Linking PT {pt_id} <-> BT {bt_id}")
                link_bank_transaction(bt.get('transaction_id'), pt_id)

def execute_three_way_match(match: ThreeWayMatch, player_id: int, dry_run: bool = False):
    """
    Executes a three-way match by splitting the original transaction.
    """
    print(f"\n--- Executing Three-Way Match for PT {match.original_platform_transaction.get('id')} ---")
    
    original_pt = match.original_platform_transaction
    to_account_id = original_pt.get('To_Account') # Original Betting Bank account ID

    if dry_run:
        print(f"  [DRY RUN] Would update original PT {original_pt.get('id')} to gross amount ${match.gross_amount} and point to Transfer Account.")
        print(f"  [DRY RUN] Would create new 'fees' transaction for ${match.fee_amount}.")
        print(f"  [DRY RUN] Would create new 'transfer' transaction for ${match.net_amount} to original Betting Bank account.")
        if match.paypal_bank_transaction:
            print(f"  [DRY RUN] Would link PayPal BT {match.paypal_bank_transaction.get('id')} to updated PT {original_pt.get('id')}.")
        if match.bank_side_transaction:
            print(f"  [DRY RUN] Would link Bank-side BT {match.bank_side_transaction.get('id')} to the new transfer PT.")
        else:
            print(f"  [DRY RUN] Bank-side BT not found, would not be linked.")
        return

    # 1. Update the original transaction: Betting PayPal -> Transfer Account (Gross Amount)
    update_payload = {
        'Amount': match.gross_amount,
        'To_Account': TRANSFER_ACCOUNT_ID,
        'Notes': f"System Matched (3-way): Amount updated to Gross. Destination updated to Transfer Account on {datetime.now().isoformat()}"
    }
    updated_pt = update_platform_transaction(original_pt.get('id'), update_payload)
    if not updated_pt:
        print(f"  ERROR: Failed to update original PT {original_pt.get('id')}. Aborting three-way execution for this match.")
        return

    # 2. Create the fees transaction: Transfer Account -> Fees Account
    fees_payload = {
        'Transaction_Type': 'fees',
        'Amount': match.fee_amount,
        'Date': match.scraped_transaction.get('Transaction Date'),
        'From_Account': TRANSFER_ACCOUNT_ID,
        'To_Account': FEES_ACCOUNT_ID,
        'User_ID': player_id,
        'Added_By': AI_USER_ID,
    }
    fees_pt = create_platform_transaction(fees_payload)
    if not fees_pt:
        print("  ERROR: Failed to create fees transaction. The original transaction has been updated, but the split is incomplete.")
        return
        
    # 3. Create the net transfer transaction: Transfer Account -> Betting Bank
    transfer_payload = {
        'Transaction_Type': 'transfer',
        'Amount': match.net_amount,
        'Date': match.scraped_transaction.get('Transaction Date'),
        'From_Account': TRANSFER_ACCOUNT_ID,
        'To_Account': to_account_id, # Use the original 'To_Account' ID
        'User_ID': player_id,
        'Added_By': AI_USER_ID,
    }
    transfer_pt = create_platform_transaction(transfer_payload)
    if not transfer_pt:
        print("  ERROR: Failed to create net transfer transaction. The original transaction has been updated, but the split is incomplete.")
        return

    # 4. Match the transactions
    print("  Linking bank transactions to new/updated platform transactions...")
    if match.paypal_bank_transaction:
        link_bank_transaction(match.paypal_bank_transaction.get('transaction_id'), updated_pt.get('id'))
    if match.bank_side_transaction:
        link_bank_transaction(match.bank_side_transaction.get('transaction_id'), transfer_pt.get('id'))
    
    print(f"  SUCCESS: Successfully executed three-way match for original PT {original_pt.get('id')}")

# --- Runner Logic ---

def run_matching_for_player(player_id: int, base_output_dir: str, delay_seconds: int = 0, dry_run: bool = False):
    """Main function to run the simplified matching logic for a single player."""
    if delay_seconds > 0:
        time.sleep(delay_seconds)

    print(f"\nStarting simplified matching for player {player_id}")
    
    output_dir = os.path.join(base_output_dir, f"player_{player_id}")
    os.makedirs(output_dir, exist_ok=True)
    
    # 1. Fetch Data
    platform_transactions = get_all_platform_transactions(player_id)
    bank_transactions = get_all_bank_transactions(player_id)
    scraped_transactions = get_scraped_transactions(player_id)
    
    # Save raw data for inspection
    raw_data_dir = os.path.join(output_dir, "raw_data")
    os.makedirs(raw_data_dir, exist_ok=True)
    with open(os.path.join(raw_data_dir, "platform_transactions.json"), 'w') as f:
        json.dump(platform_transactions, f, indent=2)
    with open(os.path.join(raw_data_dir, "bank_transactions.json"), 'w') as f:
        json.dump(bank_transactions, f, indent=2)
    with open(os.path.join(raw_data_dir, "scraped_transactions.json"), 'w') as f:
        json.dump(scraped_transactions, f, indent=2)

    # 2. Run Matcher
    matcher = SimplifiedPayPalMatcher(platform_transactions, bank_transactions, scraped_transactions)
    results = matcher.match_transactions()
    
    print(f"  Matching Results: {len(results.simple_matches)} simple, {len(results.three_way_matches)} three-way, {len(results.unmatched_platform_transactions)} unmatched.")
    
    # 3. Execute Matches
    execute_simple_matches(results.simple_matches, dry_run)
    for match in results.three_way_matches:
        execute_three_way_match(match, player_id, dry_run)
        
    # 4. Save Report
    report_path = os.path.join(output_dir, "simplified_matching_report.json")
    report_data = {
        "player_id": player_id,
        "run_timestamp": datetime.now().isoformat(),
        "simple_matches": [m.__dict__ for m in results.simple_matches],
        "three_way_matches": [m.__dict__ for m in results.three_way_matches],
        "unmatched_transactions": [u.__dict__ for u in results.unmatched_platform_transactions]
    }
    with open(report_path, 'w') as f:
        json.dump(report_data, f, indent=2, default=str)
        
    print(f"  Report saved to {report_path}")

# --- API Interaction Functions ---

def link_bank_transaction(bank_transaction_id: str, platform_transaction_id: int):
    """Links a bank transaction to a platform transaction."""
    url = UPDATE_BANK_TRANSACTIONS_API_URL.format(transaction_id=bank_transaction_id)
    payload = {"transaction_link": platform_transaction_id, "last_edited_by": AI_USER_ID}
    try:
        response = requests.patch(url, json=payload, timeout=30)
        response.raise_for_status()
        print(f"    - LINK SUCCESS: BT {bank_transaction_id} -> PT {platform_transaction_id}")
        return True
    except requests.RequestException as e:
        print(f"    - LINK FAILED for BT {bank_transaction_id}: {e}")
        return False

def update_platform_transaction(pt_id: int, changes: dict):
    """Updates an existing platform transaction."""
    url = UPDATE_PLATFORM_TRANSACTIONS_API_URL.format(platform_transaction_id=pt_id)
    try:
        response = requests.patch(url, json=changes, timeout=30)
        response.raise_for_status()
        print(f"  UPDATE SUCCESS: PT {pt_id}")
        return response.json()
    except requests.RequestException as e:
        print(f"  UPDATE FAILED for PT {pt_id}: {e}")
        return None

def create_platform_transaction(payload: dict):
    """Creates a new platform transaction."""
    try:
        response = requests.post(CREATE_PLATFORM_TRANSACTION_API_URL, json=payload, timeout=30)
        response.raise_for_status()
        new_pt = response.json()
        print(f"  CREATE SUCCESS: New PT ID {new_pt.get('id')} ({payload.get('Transaction_Type')}, ${payload.get('Amount')})")
        return new_pt
    except requests.RequestException as e:
        print(f"  CREATE FAILED for new {payload.get('Transaction_Type')} transaction: {e}")
        return None

# --- Main Execution Block ---

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Run the simplified PayPal to Bank matcher.")
    parser.add_argument("player_ids", nargs='+', type=int, help="One or more player IDs to process.")
    parser.add_argument("--dry-run", action="store_true", help="Run the matcher without executing any transactions (read-only).")
    parser.add_argument("--max-workers", type=int, default=5, help="Maximum number of concurrent players to process.")
    parser.add_argument("--delay", type=int, default=2, help="Delay in seconds between starting each player's task.")
    
    args = parser.parse_args()

    player_ids_to_process = args.player_ids
    MAX_WORKERS = args.max_workers
    DELAY_BETWEEN_TASKS = args.delay

    if args.dry_run:
        print("\n--- RUNNING IN DRY-RUN MODE: NO DATA WILL BE MODIFIED ---\n")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    batch_base_dir = f"batch_run_simplified_{timestamp}"
    os.makedirs(batch_base_dir, exist_ok=True)
    print(f"Batch output will be saved in: {batch_base_dir}")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(run_matching_for_player, pid, batch_base_dir, i * DELAY_BETWEEN_TASKS, args.dry_run)
            for i, pid in enumerate(player_ids_to_process)
        ]
        for i, future in enumerate(futures):
            future.result()  # Wait for completion and handle exceptions
            print(f"--- Completed processing player {i + 1}/{len(player_ids_to_process)} ---")

    print("\n--- Batch processing complete. ---")
