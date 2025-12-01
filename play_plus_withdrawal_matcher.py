#!/usr/bin/env python3
"""
Matches Play Plus withdrawal transactions (Load Money Onto Card) with platform transactions.
"""

import requests
import argparse
import json
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from config import (
    API_TIMEOUT,
    PLAYERS_API_URL,
    SCRAPED_TRANSACTION_API_URL,
    PLATFORM_TRANSACTIONS_API_URL,
    USER_ACCOUNTS_API_URL,
    UPDATE_PLATFORM_TRANSACTIONS_API_URL,
    UPDATE_SCRAPED_TRANSACTIONS_API_URL,
    CASINOACCOUNTS_API_URL
)


class PlayPlusWithdrawalMatcher:
    """
    Handles the logic for matching Play Plus withdrawal transactions.
    """
    def __init__(self, summary_only=False):
        self.summary_only = summary_only

    def get_player_ids(self, player_id, player_stages):
        """
        Fetches player IDs based on single ID or player stages.
        """
        if player_id:
            return [player_id]

        if not player_stages:
            return []

        if not self.summary_only:
            print("Fetching all players to filter by stage (excluding @reeledge.com)...")
        try:
            response = requests.get(PLAYERS_API_URL, timeout=60)
            response.raise_for_status()
            all_players = response.json()

            # Filter out internal accounts first
            filtered_players = [p for p in all_players if '@reeledge.com' not in p.get('email', '').lower()]
            
            # Now, filter by the requested stages
            player_ids = [p['id'] for p in filtered_players if p.get('player_stage') in player_stages and p.get('id')]

            if not self.summary_only:
                print(f"Found {len(player_ids)} players in stages {player_stages} to analyze.")
            return list(set(player_ids)) # Use set to ensure unique IDs

        except requests.exceptions.RequestException as e:
            print(f"Error fetching players: {e}")
            return []

    def fetch_scraped_transactions(self, player_id):
        """Fetches Play Plus scraped transactions for a given player."""
        if not self.summary_only:
            print(f"Fetching scraped transactions for player {player_id}...")
        try:
            response = requests.get(SCRAPED_TRANSACTION_API_URL, params={'user_id': player_id}, timeout=60)
            response.raise_for_status()
            transactions = response.json()
            playplus_transactions = [t for t in transactions if t.get('Source') == 'playplus']
            if not self.summary_only:
                print(f"Found {len(playplus_transactions)} Play Plus scraped transactions.")
            return playplus_transactions
        except requests.exceptions.RequestException as e:
            print(f"Error fetching scraped transactions for player {player_id}: {e}")
            return []

    def fetch_platform_transactions(self, player_id):
        """Fetches all platform transactions for a given player."""
        if not self.summary_only:
            print(f"Fetching platform transactions for player {player_id}...")
        try:
            response = requests.get(PLATFORM_TRANSACTIONS_API_URL, params={'user_id': player_id}, timeout=120)
            response.raise_for_status()
            transactions = response.json()
            if not self.summary_only:
                print(f"Found {len(transactions)} platform transactions.")
            return transactions
        except requests.exceptions.RequestException as e:
            print(f"Error fetching platform transactions for player {player_id}: {e}")
            return []

    def fetch_casino_accounts(self, player_id):
        """Fetches all casino accounts for a given player."""
        if not self.summary_only:
            print(f"Fetching all casino accounts for player {player_id}...")
        try:
            params = {"user_id": int(player_id)}
            response = requests.get(CASINOACCOUNTS_API_URL, params=params, timeout=60)
            response.raise_for_status()
            accounts = response.json()
            if not self.summary_only:
                print(f"Found {len(accounts)} casino accounts.")
            return accounts
        except (requests.exceptions.RequestException, ValueError) as e:
            print(f"Error fetching casino accounts for player {player_id}: {e}")
            return []

    def update_platform_transaction(self, transaction_id, data, dry_run=False):
        """Updates a platform transaction."""
        url = UPDATE_PLATFORM_TRANSACTIONS_API_URL.format(platform_transaction_id=transaction_id)
        
        update_payload = data.copy()

        if dry_run:
            print(f"[DRY RUN] Would update platform transaction {transaction_id} via PATCH to {url} with payload: {update_payload}")
            return

        if not self.summary_only:
            print(f"Updating platform transaction {transaction_id} with payload: {update_payload}")

        try:
            response = requests.patch(url, json=update_payload, timeout=API_TIMEOUT)
            response.raise_for_status()
            if not self.summary_only:
                print(f"Successfully updated platform transaction {transaction_id}.")
        except requests.exceptions.RequestException as e:
            print(f"Error updating platform transaction {transaction_id}: {e}")

    def update_scraped_transaction(self, transaction_id, data, dry_run=False):
        """Updates a scraped transaction using PATCH."""
        url = UPDATE_SCRAPED_TRANSACTIONS_API_URL.format(id=transaction_id)
        
        update_payload = data.copy()
        update_payload['id'] = transaction_id

        if dry_run:
            if not self.summary_only:
                print(f"[DRY RUN] Would update scraped transaction {transaction_id} with payload: {update_payload}")
            return None

        if not self.summary_only:
            print(f"Updating scraped transaction {transaction_id} with payload: {update_payload}")

        try:
            response = requests.patch(url, json=update_payload, timeout=API_TIMEOUT)
            response.raise_for_status()
            if not self.summary_only:
                print(f"Successfully updated scraped transaction {transaction_id}.")
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error updating scraped transaction {transaction_id}: {e}")
            return None

    def process_player(self, player_id, dry_run=False):
        """
        Orchestrates the matching process for a single player.
        """
        if not self.summary_only:
            print(f"\nProcessing player ID: {player_id}")

        scraped_transactions = self.fetch_scraped_transactions(player_id)
        if not scraped_transactions:
            if not self.summary_only:
                print(f"No Play Plus scraped transactions found for player {player_id}. Skipping.")
            return

        # Filter for "load money onto card" transactions
        withdrawal_transactions = [
            st for st in scraped_transactions 
            if "load money onto card" in st.get('Description', '').lower()
            and not st.get('transaction_link')
            and (st.get('casinoaccounts_id') or st.get('Casino Accounts ID'))
        ]

        if not withdrawal_transactions:
            if not self.summary_only:
                print(f"No unmatched Play Plus withdrawal transactions found for player {player_id}.")
            return

        if not self.summary_only:
            print(f"Found {len(withdrawal_transactions)} unmatched Play Plus withdrawal transactions to process.")

        platform_transactions = self.fetch_platform_transactions(player_id)
        casino_accounts = self.fetch_casino_accounts(player_id)

        if not casino_accounts:
            if not self.summary_only:
                print(f"Warning: No casino accounts found for player {player_id}. Cannot determine correct casino_id.")
            return

        casino_account_map = {acc['id']: acc.get('casino_id') for acc in casino_accounts if acc.get('id')}

        for st in withdrawal_transactions:
            try:
                scraped_amount = abs(float(st.get('Net', 0)))
                scraped_date_str = st.get('Transaction Date')
                if not scraped_date_str or scraped_date_str == "N/A":
                    continue
                scraped_date = datetime.strptime(scraped_date_str, '%Y-%m-%d')
                
                scraped_casino_account_id_str = st.get('casinoaccounts_id') or st.get('Casino Accounts ID')
                if not scraped_casino_account_id_str:
                    continue
                scraped_casino_account_id = int(scraped_casino_account_id_str)
                correct_casino_id = casino_account_map.get(scraped_casino_account_id)
                if not correct_casino_id:
                    if not self.summary_only:
                        print(f"Could not map casino account ID {scraped_casino_account_id} to a casino_id for ST {st['id']}")
                    continue

            except (ValueError, TypeError) as e:
                if not self.summary_only:
                    print(f"Could not parse data for scraped transaction {st.get('id')}: {e}")
                continue

            # Find matching platform transaction
            matching_platform_tran = None
            if platform_transactions:
                for pt in platform_transactions:
                    try:
                        # Check if already linked
                        if pt.get('related_scraped_transaction'):
                            continue

                        # Casino ID check
                        if pt.get('Casino') != correct_casino_id:
                            continue

                        # Transaction Type check
                        if pt.get('Transaction_Type', '').lower() != 'withdrawal':
                            continue

                        # Date check (+- 9 days)
                        platform_date_str = pt.get('Date')
                        if not platform_date_str or platform_date_str == "N/A": continue
                        try:
                            platform_date = datetime.strptime(platform_date_str, '%Y-%m-%d')
                        except ValueError:
                            platform_date = datetime.fromtimestamp(float(platform_date_str) / 1000)
                        
                        if abs((scraped_date - platform_date).days) > 9:
                            continue
                        
                        # Amount check
                        platform_amount = abs(float(pt.get('Amount', 0)))
                        if abs(scraped_amount - platform_amount) > 0.01:
                            continue

                        # If we get here, it's a match
                        matching_platform_tran = pt
                        break
                    except (ValueError, TypeError):
                        continue

            if matching_platform_tran:
                if not self.summary_only:
                    print(f"Found matching PT {matching_platform_tran['id']} for ST {st['id']}. Linking them.")
                
                update_payload_pt = {'related_scraped_transaction': st['id']}
                self.update_platform_transaction(matching_platform_tran['id'], update_payload_pt, dry_run)
                
                update_payload_st = {'transaction_link': matching_platform_tran['id']}
                self.update_scraped_transaction(st['id'], update_payload_st, dry_run)
            else:
                if not self.summary_only:
                    print(f"No matching platform transaction found for ST {st['id']}.")

        if not self.summary_only:
            print(f"\nFinished processing player ID: {player_id}")


    def run(self, player_id=None, player_stages=None, dry_run=False):
        """
        Main execution function.
        """
        player_ids = self.get_player_ids(player_id, player_stages)
        if not player_ids:
            print("No players to process.")
            return

        print(f"Starting analysis for {len(player_ids)} players...")

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(self.process_player, pid, dry_run): pid for pid in player_ids}

            for i, future in enumerate(as_completed(futures)):
                pid = futures[future]
                try:
                    future.result()
                except Exception as e:
                    print(f"An error occurred while processing player {pid}: {e}")
                finally:
                    if not self.summary_only:
                        print(f"--- Completed processing player {i + 1}/{len(player_ids)} ---")

        print("\n" + "="*60)
        print("--- BATCH SUMMARY ---")
        print("="*60)
        print("Processing complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Play Plus Withdrawal Matching Script")
    parser.add_argument('--player_id', type=int, help="Run for a single player ID.")
    parser.add_argument('--player_stages', nargs='+', help="Run for players in specific stages (e.g., 'active' 'new').")
    parser.add_argument('--dry_run', action='store_true', help="Run in dry run mode without making actual updates.")
    parser.add_argument('--summary_only', action='store_true', help="Suppress detailed per-player output and show only the final summary.")
    args = parser.parse_args()

    player_stages_to_run = args.player_stages
    if not args.player_id and not args.player_stages:
        try:
            stages_input = input("Please enter player stages separated by spaces (e.g., active new): ")
            if stages_input:
                player_stages_to_run = stages_input.split()
        except KeyboardInterrupt:
            print("\nOperation cancelled by user.")
            exit()

    matcher = PlayPlusWithdrawalMatcher(summary_only=args.summary_only)
    matcher.run(player_id=args.player_id, player_stages=player_stages_to_run, dry_run=args.dry_run)

