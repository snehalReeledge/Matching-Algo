import requests
import json
from datetime import datetime, timedelta
import os
from config import (
    PLAYERS_API_URL, 
    PLATFORM_TRANSACTIONS_API_URL, 
    BANK_TRANSACTIONS_API_URL, 
    UPDATE_BANK_TRANSACTIONS_API_URL,
    AI_USER_ID,
    API_TIMEOUT
)

class WithdrawalMatcher:
    """
    Matches "Withdrawal" type platform transactions with corresponding bank transactions
    by fetching live data from APIs, following established project patterns.
    """
    def __init__(self):
        self.casino_keywords = self._load_casino_keywords()

    def _load_casino_keywords(self):
        """Loads casino keywords into a dict mapping casino names to their keywords."""
        keyword_map = {}
        try:
            with open('CASINO_KEYWORDS.json', 'r', encoding='utf-8') as f:
                data = json.load(f).get('data', [])
                for item in data:
                    name = item.get('name', '').upper()
                    keywords = item.get('keyword', '')
                    # Add all keywords from the general list under a common key
                    if name and keywords:
                        # Split keywords and add to the map
                        keyword_list = [k.strip().upper() for k in keywords.split(',') if k.strip()]
                        if name in keyword_map:
                            keyword_map[name].extend(keyword_list)
                        else:
                            keyword_map[name] = keyword_list
        except (FileNotFoundError, json.JSONDecodeError) as e:
            print(f"Error loading casino keywords: {e}")
        return keyword_map

    def _fetch_data(self, url, params=None):
        """Generic function to fetch data from an API endpoint."""
        try:
            response = requests.get(url, params=params, timeout=API_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data from {url}: {e}")
            return None

    def _link_transactions(self, platform_transaction_id, bank_transaction):
        """Links a bank transaction to a platform transaction."""
        bank_transaction_id_for_api = bank_transaction.get('transaction_id')
        if not bank_transaction_id_for_api:
            print(f"ERROR: Bank transaction ID {bank_transaction.get('id')} is missing 'transaction_id' for linking.")
            return
        
        url = UPDATE_BANK_TRANSACTIONS_API_URL.format(transaction_id=bank_transaction_id_for_api)
        payload = {"transaction_link": platform_transaction_id, "last_edited_by": AI_USER_ID}
        try:
            response = requests.patch(url, json=payload)
            response.raise_for_status()
            print(f"Successfully linked BT {bank_transaction_id_for_api} to PT {platform_transaction_id}")
        except requests.exceptions.RequestException as e:
            print(f"Failed to link BT {bank_transaction_id_for_api}: {e}")

    def _log_potential_match(self, platform_transaction, bank_transaction):
        """Logs a potential match for a dry run."""
        print("\n" + "="*50)
        print(f"[DRY RUN] Found potential match:")
        print(f"  - Platform Transaction ID: {platform_transaction.get('id')}")
        print(f"    - Date: {platform_transaction.get('Date')}, Amount: {platform_transaction.get('Amount')}")
        print(f"    - From: {platform_transaction.get('from', {}).get('Account_Name')}, To: {platform_transaction.get('to', {}).get('Account_Name')}")
        print(f"  - Bank Transaction ID: {bank_transaction.get('id')}")
        print(f"    - Date: {bank_transaction.get('date')}, Amount: {bank_transaction.get('amount')}")
        print(f"    - Name: '{bank_transaction.get('name')}', Counterparty: '{bank_transaction.get('counterparty_name')}'")
        print("="*50)

    def find_matches(self, dry_run=False, player_id=None, player_stages=None, limit=None):
        """Processes transactions for players and applies matching logic."""
        mode = "(Dry Run)" if dry_run else "(Live Mode)"
        print(f"Starting withdrawal matching process... {mode}")

        if not self.casino_keywords:
            print("Cannot proceed: casino keywords are not loaded.")
            return

        players_to_process = []
        if player_id:
            players_to_process = [{'id': player_id}]
        else:
            all_players = self._fetch_data(PLAYERS_API_URL)
            if all_players:
                if player_stages:
                    players_to_process = [p for p in all_players if p.get('player_stage') in player_stages]
                    print(f"Filtered to {len(players_to_process)} players in stages: {player_stages}")
                    if limit:
                        players_to_process = players_to_process[:limit]
                        print(f"Limiting run to the first {len(players_to_process)} players.")
                else:
                    players_to_process = all_players

        if not players_to_process:
            print("No players to process after filtering. Aborting.")
            return

        total_matches = 0
        for player in players_to_process:
            current_player_id = player.get('id')
            if not current_player_id: continue

            print(f"\nProcessing player ID: {current_player_id}...")
            
            # 1. Fetch and filter unmatched platform withdrawals
            platform_params = {'user_id': current_player_id}
            all_platform_tx = self._fetch_data(PLATFORM_TRANSACTIONS_API_URL, params=platform_params) or []
            
            platform_transactions = [
                pt for pt in all_platform_tx
                if pt.get('Transaction_Type', '').lower() == 'withdrawal' and not pt.get('related_bank_transaction')
            ]

            print(f"Found {len(platform_transactions)} total unmatched withdrawals.")

            # Early exit if there are no platform withdrawals to process
            if not platform_transactions:
                print(f"No processable withdrawals found for player {current_player_id}.")
                continue

            # 2. Fetch and filter unmatched bank transactions
            bank_params = {'player_id': current_player_id}
            bank_tx_response = self._fetch_data(BANK_TRANSACTIONS_API_URL, params=bank_params) or {}
            all_bank_tx = bank_tx_response.get('bankTransactions', [])
            bank_transactions = [bt for bt in all_bank_tx if not bt.get('linked_transaction')]

            if not bank_transactions:
                print(f"No unmatched bank transactions to process for player {current_player_id}.")
                continue
            
            # Sort both lists by date in ascending order
            try:
                platform_transactions.sort(key=lambda pt: datetime.strptime(pt.get('Date'), '%Y-%m-%d'))
                bank_transactions.sort(key=lambda bt: datetime.strptime(bt.get('date'), '%Y-%m-%d'))
            except (ValueError, TypeError) as e:
                print(f"Warning: Could not sort transactions for player {current_player_id} due to a date parsing error: {e}")

            player_matches = self._match_transactions_for_player(platform_transactions, bank_transactions, dry_run)
            total_matches += player_matches
        
        print(f"\n--- Overall Matching Complete ({mode}) ---")
        print(f"Total new matches found: {total_matches}")
        if dry_run:
            print("Note: No transactions were actually linked as this was a dry run.")
        print("------------------------------------------")

    def _match_transactions_for_player(self, platform_transactions, bank_transactions, dry_run):
        """Core matching logic for a single player's pre-filtered data."""
        match_count = 0
        for pt in platform_transactions:
            try:
                platform_amount = round(float(pt.get('Amount', 0)), 2)
                platform_date = datetime.strptime(pt.get('Date'), '%Y-%m-%d')
                to_bank_account_id = pt.get('to', {}).get('bankaccount_id')
                if not to_bank_account_id: continue

                # Dynamic Keyword Validation
                pt_casino_name = pt.get('Name', '').upper()
                keywords_for_casino = self.casino_keywords.get(pt_casino_name, [])
                if not keywords_for_casino:
                    print(f"Warning: No keywords found for casino '{pt.get('Name')}' from PT {pt.get('id')}. Skipping.")
                    continue

                found_match = None
                for bt in bank_transactions:
                    bank_amount = round(float(bt.get('amount', 0)), 2)
                    bank_date = datetime.strptime(bt.get('date'), '%Y-%m-%d')
                    bank_account_id = bt.get('bankaccount_id')
                    description = f"{bt.get('name', '')} {bt.get('counterparty_name', '')}".upper()

                    # Compare rounded amounts
                    amount_match = (bank_amount == -platform_amount)
                    date_match = abs((platform_date - bank_date).days) <= 9
                    account_match = (str(bank_account_id) == str(to_bank_account_id))
                    keyword_match = any(keyword in description for keyword in keywords_for_casino)

                    # Apply all matching rules
                    if amount_match and date_match and account_match and keyword_match:
                        found_match = bt
                        break # Found the first chronological match, stop searching
                
                if found_match:
                    if dry_run:
                        self._log_potential_match(pt, found_match)
                    else:
                        self._link_transactions(pt.get('id'), found_match)
                    match_count += 1
                    bank_transactions.remove(found_match) # Prevent re-matching
            except (ValueError, TypeError) as e:
                print(f"Skipping platform transaction {pt.get('id')} due to data error: {e}")
        
        return match_count
