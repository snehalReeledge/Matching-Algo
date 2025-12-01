#!/usr/bin/env python3
"""
Deposit Matching Algorithm
Matches "Deposit" type platform transactions with their corresponding bank transactions.
"""

import requests
import argparse
import json
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

from config import (
    API_TIMEOUT,
    PLAYERS_API_URL,
    PLATFORM_TRANSACTIONS_API_URL,
    BANK_TRANSACTIONS_API_URL,
    UPDATE_BANK_TRANSACTIONS_API_URL,
    UPDATE_PLATFORM_TRANSACTIONS_API_URL
)

AI_USER_ID = 35047

# Constants
CASINO_KEYWORDS_FILE = 'CASINO_KEYWORDS.json'

class DepositMatcher:
    def __init__(self, summary_only=False):
        self.summary_only = summary_only
        self.casino_keywords = self.load_keywords()

    def load_keywords(self):
        """Loads casino keywords from JSON file."""
        try:
            with open(CASINO_KEYWORDS_FILE, 'r') as f:
                data = json.load(f)
                # Map casino name (normalized?) to list of keywords
                # The algorithm says: "Get the casino name from the platform transaction's Name field."
                # "Look up the list of associated keywords for this casino name"
                # The JSON has "name" and "keyword" fields.
                
                keywords_map = {}
                if 'data' in data:
                    for item in data['data']:
                        name = item.get('name')
                        keywords_str = item.get('keyword', '')
                        if name and keywords_str:
                            # Split by comma and strip whitespace
                            keywords = [k.strip().upper() for k in keywords_str.split(',')]
                            keywords_map[name] = keywords
                return keywords_map
        except Exception as e:
            print(f"Error loading keywords: {e}")
            return {}

    def get_player_ids(self, player_id, player_stages):
        """Fetches player IDs based on single ID or player stages."""
        if player_id:
            return [player_id]

        if not player_stages:
            return []

        if not self.summary_only:
            print("Fetching all players to filter by stage...")
        try:
            response = requests.get(PLAYERS_API_URL, timeout=60)
            response.raise_for_status()
            all_players = response.json()

            filtered_players = [p for p in all_players if '@reeledge.com' not in p.get('email', '').lower()]
            player_ids = [p['id'] for p in filtered_players if p.get('player_stage') in player_stages and p.get('id')]

            if not self.summary_only:
                print(f"Found {len(player_ids)} players in stages {player_stages}.")
            return list(set(player_ids))

        except requests.exceptions.RequestException as e:
            print(f"Error fetching players: {e}")
            return []

    def fetch_platform_transactions(self, player_id):
        """Fetches platform transactions for a player."""
        try:
            response = requests.get(PLATFORM_TRANSACTIONS_API_URL, params={'user_id': player_id}, timeout=120)
            response.raise_for_status()
            transactions = response.json()
            return transactions
        except requests.exceptions.RequestException as e:
            print(f"Error fetching platform transactions for player {player_id}: {e}")
            return []

    def fetch_bank_transactions(self, player_id):
        """Fetches all bank transactions for a player."""
        # Algorithm says "Fetch all of the player's bank transactions". 
        # Unlike play_plus_matcher which used dates, here we might want all or a wide range.
        # The API supports start_date/end_date. If omitted, does it return all?
        # Let's assume we need a reasonable range or handle pagination if needed, 
        # but based on other scripts, we might just request a wide range or omit dates if API allows.
        # However, other scripts use a date range. 
        # "Fetch all... Filter to retain only transactions where... linked_transaction is null"
        # To be safe and cover history, I'll use a wide range or rely on default API behavior if it returns all.
        # Let's try to fetch without dates first, or use a very old start date.
        
        try:
            # Using a wide range to simulate "all"
            start_date = "2020-01-01"
            end_date = datetime.now().strftime('%Y-%m-%d')
            params = {
                'player_id': player_id,
                'start_date': start_date,
                'end_date': end_date
            }
            response = requests.get(BANK_TRANSACTIONS_API_URL, params=params, timeout=120)
            response.raise_for_status()
            
            # Handle response structure
            data = response.json()
            if isinstance(data, dict) and 'bankTransactions' in data:
                return data['bankTransactions']
            elif isinstance(data, list):
                return data
            return []
        except requests.exceptions.RequestException as e:
            print(f"Error fetching bank transactions for player {player_id}: {e}")
            return []

    def update_bank_transaction(self, transaction_id, related_platform_id, dry_run=False):
        """Updates bank transaction with link."""
        url = UPDATE_BANK_TRANSACTIONS_API_URL.format(transaction_id=transaction_id)
        payload = {
            'transaction_link': related_platform_id,
            'last_edited_by': AI_USER_ID
        }

        if dry_run:
            print(f"[DRY RUN] Link BT {transaction_id} to PT {related_platform_id}")
            return

        try:
            requests.patch(url, json=payload, timeout=API_TIMEOUT)
            if not self.summary_only:
                print(f"Linked BT {transaction_id} to PT {related_platform_id}")
        except requests.exceptions.RequestException as e:
            print(f"Error updating BT {transaction_id}: {e}")

    def update_platform_transaction(self, transaction_id, related_bank_id, dry_run=False):
        """Updates platform transaction with link."""
        url = UPDATE_PLATFORM_TRANSACTIONS_API_URL.format(platform_transaction_id=transaction_id)
        payload = {'related_bank_transaction': [related_bank_id]}

        if dry_run:
            print(f"[DRY RUN] Link PT {transaction_id} to BT {related_bank_id}")
            return

        try:
            requests.patch(url, json=payload, timeout=API_TIMEOUT)
            if not self.summary_only:
                print(f"Linked PT {transaction_id} to BT {related_bank_id}")
        except requests.exceptions.RequestException as e:
            print(f"Error updating PT {transaction_id}: {e}")

    def parse_date(self, date_val):
        """Parses date from string or timestamp."""
        if not date_val:
            return None
        try:
            if isinstance(date_val, (int, float)):
                return datetime.fromtimestamp(date_val / 1000)
            return datetime.strptime(str(date_val), '%Y-%m-%d')
        except ValueError:
            return None

    def process_player(self, player_id, dry_run=False):
        if not self.summary_only:
            print(f"\nProcessing player {player_id}...")

        # 1. Fetch and Filter Platform Transactions
        all_pt = self.fetch_platform_transactions(player_id)
        
        # Filter: Transaction_Type == 'deposit' AND related_bank_transaction is empty
        deposit_pts = []
        for pt in all_pt:
            t_type = pt.get('Transaction_Type', '').lower()
            related = pt.get('related_bank_transaction')
            
            # Check if already linked (empty list, null, etc.)
            is_unlinked = not related or (isinstance(related, list) and len(related) == 0)
            
            if t_type == 'deposit' and is_unlinked:
                deposit_pts.append(pt)

        if not deposit_pts:
            if not self.summary_only:
                print(f"No unlinked deposit platform transactions for player {player_id}.")
            return

        # 2. Fetch and Filter Bank Transactions
        all_bt = self.fetch_bank_transactions(player_id)
        
        # Filter: transaction_link is null
        unlinked_bts = []
        for bt in all_bt:
            if not bt.get('transaction_link'):
                unlinked_bts.append(bt)

        # 3. Sort for Determinism (by Date ascending)
        # Helper to get date for sorting safely
        def get_date(item, date_field):
            d = self.parse_date(item.get(date_field))
            return d if d else datetime.min

        deposit_pts.sort(key=lambda x: get_date(x, 'Date'))
        unlinked_bts.sort(key=lambda x: get_date(x, 'date'))

        matches_found = 0

        # 4. Matching Logic
        # Iterate through platform transactions
        for pt in deposit_pts:
            pt_id = pt.get('id')
            
            # Pre-computation for PT
            try:
                pt_amount = round(float(pt.get('Amount', 0)), 2)
                pt_date = self.parse_date(pt.get('Date'))
                if not pt_date:
                    continue
                
                # Identify from account's bankaccount_id
                # The 'from' field is usually an object or ID. 
                # Based on other scripts/memory: "from": { "bankaccount_id": 1647, ... }
                from_acc = pt.get('from')
                if isinstance(from_acc, dict):
                    pt_bank_account_id = from_acc.get('bankaccount_id')
                else:
                    # If it's just an ID or missing structure, we can't match based on account ID
                    pt_bank_account_id = None
                
                if not pt_bank_account_id:
                    continue

                pt_casino_name = pt.get('Name') # Casino name from Name field
                
            except (ValueError, TypeError):
                continue

            # Search for match in available bank transactions
            match_index = -1
            
            for idx, bt in enumerate(unlinked_bts):
                try:
                    # Pre-computation for BT
                    bt_amount = round(float(bt.get('amount', 0)), 2)
                    
                    # Rule 1: Amount Match (Exact positive match)
                    # pt.Amount should be positive for deposit? Usually yes.
                    # "bt.amount == pt.Amount"
                    if bt_amount != pt_amount:
                        continue
                        
                    # Rule 2: Date Proximity (9 days)
                    bt_date = self.parse_date(bt.get('date'))
                    if not bt_date:
                        continue
                    
                    if abs((bt_date - pt_date).days) > 9:
                        continue

                    # Rule 3: Account Match
                    bt_bank_account_id = bt.get('bankaccount_id')
                    if bt_bank_account_id != pt_bank_account_id:
                        continue

                    # Rule 4: Dynamic Keyword Validation
                    # bt.description (standardized) must contain keyword
                    bt_name = bt.get('name', '') or ''
                    bt_counterparty = bt.get('counterparty_name', '') or ''
                    bt_description = (bt_name + bt_counterparty).upper()
                    
                    # Look up keywords for pt_casino_name
                    # If pt_casino_name matches a key in self.casino_keywords
                    # Note: Keys in JSON might differ slightly from pt['Name']. 
                    # The algorithm says "Get the casino name from the platform transaction's Name field."
                    # We'll assume exact match or direct lookup.
                    
                    keywords = self.casino_keywords.get(pt_casino_name, [])
                    if not keywords:
                        # Maybe try case-insensitive lookup if direct fail?
                        # Or maybe the Name field is "FanDuel" and key is "FanDuel"
                        # Let's try direct first.
                        pass

                    keyword_match = False
                    for kw in keywords:
                        if kw in bt_description:
                            keyword_match = True
                            break
                    
                    if not keyword_match:
                        continue

                    # If all rules pass
                    match_index = idx
                    break

                except (ValueError, TypeError):
                    continue

            if match_index != -1:
                # Found a match
                matched_bt = unlinked_bts[match_index]
                
                bt_plaid_id = matched_bt.get('transaction_id')
                bt_xano_id = matched_bt.get('id')

                if bt_plaid_id:
                    self.update_bank_transaction(bt_plaid_id, pt_id, dry_run)
                else:
                    print(f"Error: Missing transaction_id for BT {bt_xano_id}, cannot link bank side.")
                
                self.update_platform_transaction(pt_id, bt_xano_id, dry_run)
                
                # Remove from pool to prevent re-matching
                unlinked_bts.pop(match_index)
                matches_found += 1

        if not self.summary_only:
            print(f"Player {player_id}: Matched {matches_found} deposit transactions.")

    def run(self, player_id=None, player_stages=None, dry_run=False):
        player_ids = self.get_player_ids(player_id, player_stages)
        
        if not player_ids:
            print("No players to process.")
            return

        print(f"Starting deposit matching for {len(player_ids)} players...")
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(self.process_player, pid, dry_run): pid for pid in player_ids}
            
            for future in as_completed(futures):
                pid = futures[future]
                try:
                    future.result()
                except Exception as e:
                    print(f"Error processing player {pid}: {e}")

        print("Processing complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Deposit Matching Algorithm")
    parser.add_argument('--player_id', type=int, help="Run for a single player ID")
    parser.add_argument('--player_stages', nargs='+', help="Run for players in stages")
    parser.add_argument('--dry_run', action='store_true', help="Dry run mode")
    parser.add_argument('--summary_only', action='store_true', help="Summary only")
    
    args = parser.parse_args()
    
    matcher = DepositMatcher(summary_only=args.summary_only)
    matcher.run(player_id=args.player_id, player_stages=args.player_stages, dry_run=args.dry_run)

