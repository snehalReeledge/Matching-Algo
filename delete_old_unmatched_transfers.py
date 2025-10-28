#!/usr/bin/env python3
"""
Finds and deletes old, unmatched platform transfer transactions.

A transaction is considered for deletion if it meets the following criteria:
- The transaction date is more than 15 days ago.
- It is not linked to any bank transaction (related_bank_transaction is empty or null).
- The transfer is from a 'betting bank account' to a 'betting paypal account'.

This script runs in a "dry run" mode by default, printing the transactions
that would be deleted. Use the --execute flag to perform the deletion.
"""

import argparse
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import os
import json

from config import (
    PLAYERS_API_URL,
    PLATFORM_TRANSACTIONS_API_URL,
    DELETE_PLATFORM_TRANSACTIONS_API_URL,
)

def get_players_by_stage(stage: str) -> list:
    """Fetches a list of player IDs for a given stage."""
    print(f"Fetching players for stage: '{stage}'...")
    try:
        response = requests.get(PLAYERS_API_URL)
        response.raise_for_status()
        players = [p['id'] for p in response.json() if p.get('player_stage') == stage]
        print(f"Found {len(players)} players for stage '{stage}'.")
        return players
    except requests.RequestException as e:
        print(f"Error fetching players: {e}")
        return []

def get_all_platform_transactions(player_id: int) -> list:
    """Fetches all platform transactions for a given player."""
    try:
        params = {'user_id': player_id}
        response = requests.get(PLATFORM_TRANSACTIONS_API_URL, params=params)
        response.raise_for_status()
        return response.json()
    except (requests.RequestException, ValueError) as e:
        print(f"Error fetching platform transactions for player {player_id}: {e}")
        return []

def bulk_delete_platform_transactions(transaction_ids: list[int]):
    """Bulk deletes platform transactions by sending a list of IDs."""
    if not transaction_ids:
        print("No transaction IDs provided for bulk deletion.")
        return False
    
    try:
        # The payload should be a JSON object with a 'transaction_ids' key
        payload = {"transaction_ids": transaction_ids}
        response = requests.delete(DELETE_PLATFORM_TRANSACTIONS_API_URL, json=payload)
        
        # Add detailed logging of the response
        print(f"  -> API Response Status Code: {response.status_code}")
        try:
            print(f"  -> API Response Body: {response.json()}")
        except json.JSONDecodeError:
            print(f"  -> API Response Body (not JSON): {response.text}")

        response.raise_for_status()
        print(f"  -> Successfully sent bulk delete request for {len(transaction_ids)} transactions.")
        return True
    except requests.exceptions.RequestException as e:
        print(f"  -> FAILED to send bulk delete request: {e}")
        return False

def find_transactions_to_delete(player_id: int):
    """
    Finds old, unmatched transfer transactions for a single player.
    """
    print(f"--- Processing Player ID: {player_id} ---")
    
    platform_transactions = get_all_platform_transactions(player_id)
    if not platform_transactions:
        print("  -> No platform transactions found or failed to fetch. Skipping.")
        return []

    transactions_to_delete = []
    fifteen_days_ago = datetime.now() - timedelta(days=15)

    for pt in platform_transactions:
        try:
            # 1. Check transaction date
            transaction_date = datetime.strptime(pt.get('Date', '').split('T')[0], '%Y-%m-%d')
            if transaction_date >= fifteen_days_ago:
                continue

            # 2. Check for related bank transactions
            related_bts = pt.get('related_bank_transaction', [])
            if related_bts and len(related_bts) > 0:
                continue

            # 3. Check account types
            from_account_type = pt.get('from', {}).get('Account_Type', '').lower()
            to_account_type = pt.get('to', {}).get('Account_Type', '').lower()
            
            is_correct_transfer_type = (
                from_account_type == 'betting bank account' and
                to_account_type == 'betting paypal account'
            )

            if is_correct_transfer_type:
                transactions_to_delete.append(pt)

        except (ValueError, TypeError):
            # Ignore transactions with invalid date formats
            continue
            
    if not transactions_to_delete:
        print("  -> No transactions found matching the deletion criteria.")
        return []

    print(f"  -> Found {len(transactions_to_delete)} transactions that can be deleted.")
    
    return transactions_to_delete


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Finds and deletes old, unmatched platform transfer transactions."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--player_stages", nargs='+', type=str, help="One or more player stages to process (e.g., 'Batch 1' 'Batch 2').")
    group.add_argument("--player_id", type=int, help="A specific player ID to process.")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Execute the deletion of transactions. Defaults to a dry run."
    )
    args = parser.parse_args()

    player_ids_to_process = []
    if args.player_id:
        player_ids_to_process.append(args.player_id)
    elif args.player_stages:
        players_to_process = []
        for stage in args.player_stages:
            players = get_players_by_stage(stage)
            player_ids_to_process.extend([p['id'] for p in players])

    if not player_ids_to_process:
        print("No players to process. Exiting.")
        exit()
        
    print(f"\nStarting processing for {len(player_ids_to_process)} player(s).")
    print(f"EXECUTE mode is {'ON' if args.execute else 'OFF (Dry Run)'}.")

    all_transactions_for_report = []

    for player_id in player_ids_to_process:
        transactions_to_delete = find_transactions_to_delete(player_id)
        
        if not transactions_to_delete:
            continue

        if args.execute:
            print(f"\n--- EXECUTE mode: Deleting {len(transactions_to_delete)} transactions for player {player_id}. ---")
            transaction_ids = [pt['id'] for pt in transactions_to_delete]
            bulk_delete_platform_transactions(transaction_ids)
        else:
            all_transactions_for_report.extend(transactions_to_delete)

    if not args.execute:
        if all_transactions_for_report:
            print(f"\n--- DRY RUN mode is ON. Found {len(all_transactions_for_report)} total transactions to delete. ---")
            output_dir = "debug_delete_transfer_reports"
            os.makedirs(output_dir, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            if args.player_id:
                filename = f"deletable_transfers_player_{args.player_id}_{timestamp}.json"
            else:
                stages_str = "_".join(s.replace(' ', '_') for s in args.player_stages)
                filename = f"deletable_transfers_stage_{stages_str}_{timestamp}.json"
            
            filepath = os.path.join(output_dir, filename)
            
            with open(filepath, 'w') as f:
                json.dump(all_transactions_for_report, f, indent=4)
            
            print(f"\n✅ Dry run complete. Report saved to: {filepath}")
        else:
            print("\n--- All processing complete. No transactions met the criteria. ---")

    print("\n--- All processing complete. ---")
