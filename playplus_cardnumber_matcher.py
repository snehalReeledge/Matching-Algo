#!/usr/bin/env python3
"""
Matches Play Plus scraped transactions with platform transactions to identify casino accounts.
"""

import requests
import argparse
import json
import re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from config import (
    API_TIMEOUT,
    PLAYERS_API_URL,
    SCRAPED_TRANSACTION_API_URL,
    PLATFORM_TRANSACTIONS_API_URL,
    CASINOACCOUNTS_API_URL,
    UPDATE_CASINO_ACCOUNTS_API_URL,
    UPDATE_PLATFORM_TRANSACTIONS_API_URL,
    USER_ACCOUNTS_API_URL,
    UPDATE_SCRAPED_TRANSACTIONS_API_URL
)


class PlayPlusMatcher:
    """
    Handles the logic for matching Play Plus transactions to casino accounts.
    """
    def __init__(self, summary_only=False):
        self.summary_only = summary_only

    def _load_casino_data(self):
        """Loads casino data from JSON, mapping ID to name."""
        casino_map = {}
        try:
            with open('CASINO_KEYWORDS.json', 'r', encoding='utf-8') as f:
                data = json.load(f).get('data', [])
                for item in data:
                    casino_id = item.get('id')
                    name = item.get('name')
                    if casino_id and name:
                        casino_map[casino_id] = name
        except (FileNotFoundError, json.JSONDecodeError) as e:
            print(f"Error loading casino keywords: {e}")
        return casino_map

    def fetch_all_players(self):
        """Fetches all players, excluding internal accounts."""
        if not self.summary_only:
            print("Fetching all players (excluding @reeledge.com)...")
        try:
            response = requests.get(PLAYERS_API_URL, timeout=60)
            response.raise_for_status()
            
            all_players = response.json()
            filtered_players = [p for p in all_players if '@reeledge.com' not in p.get('email', '').lower()]
            
            if not self.summary_only:
                print(f"Found {len(filtered_players)} players to analyze.")
            return filtered_players
        except requests.exceptions.RequestException as e:
            print(f"Fatal: Could not fetch players. {e}")
            return []

    def fetch_players_by_stage(self, stage):
        """Fetches all players for a given stage, excluding internal accounts."""
        if not self.summary_only:
            print(f"Fetching '{stage}' players (excluding @reeledge.com)...")
        try:
            response = requests.get(PLAYERS_API_URL, timeout=60)
            response.raise_for_status()
            
            all_players = response.json()
            stage_players = [p for p in all_players if p.get('player_stage') == stage]
            filtered_players = [p for p in stage_players if '@reeledge.com' not in p.get('email', '').lower()]
            
            if not self.summary_only:
                print(f"Found {len(filtered_players)} '{stage}' players to analyze.")
            return filtered_players
        except requests.exceptions.RequestException as e:
            print(f"Fatal: Could not fetch players. {e}")
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

    def fetch_user_accounts(self, player_id):
        """Fetches all bank accounts for a given player."""
        if not self.summary_only:
            print(f"Fetching user bank accounts for player {player_id}...")
        
        try:
            params = {"user_id": int(player_id)}
            response = requests.get(USER_ACCOUNTS_API_URL, params=params, timeout=API_TIMEOUT)
            response.raise_for_status()
            accounts = response.json()
            if not self.summary_only:
                print(f"Found {len(accounts)} user bank accounts for player {player_id}.")
            return accounts
        except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
            print(f"Error fetching user bank accounts for player {player_id}: {e}")
            return []

    def update_casino_account(self, account_id, data, dry_run=False):
        """Updates a Play Plus casino account using PATCH."""
        url = UPDATE_CASINO_ACCOUNTS_API_URL
        
        # Construct the payload for the PATCH request
        update_payload = {
            "id": account_id
        }
        update_payload.update(data)
        
        if not self.summary_only:
            print(f"Updating casino account {account_id} with data: {update_payload}")
        
        if dry_run:
            if not self.summary_only:
                print(f"[DRY RUN] Not updating casino account {account_id}.")
            return None

        try:
            response = requests.patch(url, json=update_payload, timeout=60)
            response.raise_for_status()
            if not self.summary_only:
                print(f"Successfully updated casino account {account_id}.")
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error updating casino account {account_id}: {e}")
            return None

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
        
        # Per user instruction, the payload should contain the id
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

    def _is_match(self, scraped_tran, platform_tran):
        """
        Checks for a withdrawal match (Casino -> Play+).
        Now includes detailed logging for mismatches.
        """
        # Check transaction type
        if platform_tran.get('Transaction_Type') != 'withdrawal':
            return False

        # Check account types
        from_account_type = platform_tran.get('from', {}).get('Account_Type', '').lower()
        to_account_type = platform_tran.get('to', {}).get('Account_Type', '').lower()

        if not ('casino account' in from_account_type and 'play+ account' in to_account_type):
            return False
            
        # Check amount
        net_amount = scraped_tran.get('Net')
        if net_amount is None:
            return False
        
        try:
            amount_diff = abs(float(net_amount) - abs(float(platform_tran.get('Amount', 0))))
            if amount_diff > 10:
                return False
        except (ValueError, TypeError):
            return False

        # Check date
        try:
            scraped_date_str = scraped_tran.get('Transaction Date')
            platform_date_str = platform_tran.get('Date')
            
            if not scraped_date_str or not platform_date_str or scraped_date_str == "N/A" or platform_date_str == "N/A":
                return False

            scraped_date = datetime.strptime(scraped_date_str, '%Y-%m-%d')
            
            # Platform date can be in two formats
            try:
                platform_date = datetime.strptime(platform_date_str, '%Y-%m-%d')
            except ValueError:
                # If the first format fails, it might be a Unix timestamp
                platform_date_unix = float(platform_date_str)
                platform_date = datetime.fromtimestamp(platform_date_unix / 1000)

            date_diff = abs((scraped_date - platform_date).days)
            if date_diff > 9:
                return False
        except (ValueError, TypeError):
            return False

        return True

    def _is_deposit_match(self, scraped_tran, platform_tran):
        """Checks for a deposit match (Play+ -> Casino)."""
        net_amount = scraped_tran.get('Net')
        if net_amount is None:
            return False
            
        scraped_amount = abs(float(net_amount))
        platform_amount = abs(float(platform_tran.get('Amount', 0)))

        if abs(scraped_amount - platform_amount) > 10:
            return False

        try:
            scraped_date_str = scraped_tran.get("Transaction Date")
            platform_date_str = platform_tran.get("Date")
            
            if not scraped_date_str or not platform_date_str or scraped_date_str == "N/A" or platform_date_str == "N/A":
                return False

            scraped_date = datetime.strptime(scraped_date_str, '%Y-%m-%d')
            
            try:
                platform_date = datetime.strptime(platform_date_str, '%Y-%m-%d')
            except ValueError:
                platform_date_unix = float(platform_date_str) / 1000
                platform_date = datetime.fromtimestamp(platform_date_unix)

            if abs((scraped_date - platform_date).days) > 9:
                return False
        except (ValueError, TypeError):
            return False

        from_account_type = platform_tran.get('from', {}).get('Account_Type', '').lower()
        to_account_type = platform_tran.get('to', {}).get('Account_Type', '').lower()

        if not ('play+ account' in from_account_type and 'casino account' in to_account_type):
            return False

        if platform_tran.get('Transaction_Type') != 'deposit':
            return False
            
        return True

    def _is_correction_match(self, scraped_tran, platform_tran):
        """
        Checks for a correction match (miscategorized withdrawal).
        Casino -> Betting Bank instead of Casino -> Play+
        """
        # Ensure the transaction is not already linked to a bank transaction.
        if platform_tran.get('_related_bank_transaction'):
            return False

        scraped_amount = float(scraped_tran.get('Net') or 0)
        platform_amount = abs(float(platform_tran.get('Amount') or 0))
        
        if abs(scraped_amount - platform_amount) > 0.01:
            return False

        try:
            scraped_date_str = scraped_tran.get("Transaction Date")
            platform_date_str = platform_tran.get("Date")
            
            if not scraped_date_str or not platform_date_str or scraped_date_str == "N/A" or platform_date_str == "N/A":
                return False

            scraped_date = datetime.strptime(scraped_date_str, '%Y-%m-%d')
            
            try:
                platform_date = datetime.strptime(platform_date_str, '%Y-%m-%d')
            except ValueError:
                platform_date_unix = float(platform_date_str) / 1000
                platform_date = datetime.fromtimestamp(platform_date_unix)
                
            if abs((scraped_date - platform_date).days) > 9:
                return False
        except (ValueError, TypeError):
            return False

        from_account_type = platform_tran.get('from', {}).get('Account_Type', '').lower()
        to_account_type = platform_tran.get('to', {}).get('Account_Type', '').lower()

        if not ('casino account' in from_account_type and 'betting bank account' in to_account_type):
            return False

        if platform_tran.get('Transaction_Type') != 'withdrawal':
            return False
            
        return True

    def _is_withdrawal(self, scraped_tran):
        """Checks if a scraped transaction is a withdrawal."""
        full_text = (scraped_tran.get('Name', '') + ' ' + scraped_tran.get('Description', '')).lower()
        return "credit funds" in full_text and "load money onto card" in full_text

    def _is_deposit(self, scraped_tran):
        """
        Checks if a scraped transaction looks like a deposit based on its description.
        Per user instruction, this uses the same description as a withdrawal.
        """
        description = scraped_tran.get('Description', '').lower()
        return 'credit funds' in description and 'load money onto card' in description

    def _is_card_upgrade_from(self, scraped_tran):
        """Checks if a scraped transaction is a 'card upgrade from'."""
        description = scraped_tran.get('Description', '').lower()
        return 'card upgrade - from card number' in description

    def _is_card_upgrade_to(self, scraped_tran):
        """Checks if a scraped transaction is a 'card upgrade to'."""
        description = scraped_tran.get('Description', '').lower()
        return 'card upgrade - to card number' in description

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

    def process_player(self, player_id, dry_run=False):
        """
        Orchestrates the matching process for a single player.
        Returns a dictionary indicating if the player has unmatched cards.
        """
        if not self.summary_only:
            print(f"\nProcessing player ID: {player_id}")

        scraped_transactions = self.fetch_scraped_transactions(player_id)
        if not scraped_transactions:
            if not self.summary_only:
                print(f"No Play Plus scraped transactions found for player {player_id}. Skipping.")
            return {'unmatched': False}

        platform_transactions = self.fetch_platform_transactions(player_id)
        if not platform_transactions:
            if not self.summary_only:
                print(f"No platform transactions found for player {player_id}.")
            # We might still have unmatched cards, so don't exit early
        
        casino_accounts = self.fetch_casino_accounts(player_id)
        if not casino_accounts:
            if not self.summary_only:
                print(f"No casino accounts found for player {player_id}.")
            # We might still have unmatched cards, so don't exit early

        user_accounts = self.fetch_user_accounts(player_id)
        playplus_account_id = None
        for acc in user_accounts:
            if acc and 'play+ account' in acc.get('Account_Type', '').lower():
                playplus_account_id = acc.get('id')
                if not self.summary_only:
                    print(f"Found Play+ account ID for player {player_id}: {playplus_account_id}")
                break
        
        if not playplus_account_id and not self.summary_only:
            print(f"Warning: No Play+ account found for player {player_id} among user accounts.")

        # --- Stage 1: Collect all unique card numbers and identify upgrade pairs ---
        all_card_numbers = set()
        card_upgrade_pairs = []

        # First, separate transactions into categories
        withdrawal_trans = []
        deposit_trans = []
        upgrade_from_trans = []
        upgrade_to_trans = []

        for st in scraped_transactions:
            if self._is_card_upgrade_from(st):
                upgrade_from_trans.append(st)
            elif self._is_card_upgrade_to(st):
                upgrade_to_trans.append(st)
            elif self._is_withdrawal(st):
                withdrawal_trans.append(st)
            elif self._is_deposit(st):
                deposit_trans.append(st)

        # Add all card numbers from relevant transactions to the set
        for st in withdrawal_trans + deposit_trans + upgrade_from_trans + upgrade_to_trans:
            if st.get('Card Number'):
                all_card_numbers.add(st.get('Card Number'))

        # Now, pair up the upgrade transactions
        used_to_trans_ids = set()
        for from_tran in upgrade_from_trans:
            from_card = from_tran.get('Card Number')
            from_date = from_tran.get('Transaction Date')
            try:
                from_net = float(from_tran.get('Net', 0))
            except (ValueError, TypeError):
                continue

            if not from_card or not from_date:
                continue

            for to_tran in upgrade_to_trans:
                if to_tran.get('id') in used_to_trans_ids:
                    continue
                
                to_card = to_tran.get('Card Number')
                to_date = to_tran.get('Transaction Date')
                try:
                    to_net = float(to_tran.get('Net', 0))
                except (ValueError, TypeError):
                    continue

                if not to_card or not to_date:
                    continue
                
                # Match on same date and opposite net amount
                if from_date == to_date and from_net == -to_net:
                    card_upgrade_pairs.append((from_card, to_card))
                    used_to_trans_ids.add(to_tran.get('id'))
                    if not self.summary_only:
                        print(f"Found card upgrade pair: {from_card} -> {to_card}")
                    break

        if not all_card_numbers:
            if not self.summary_only:
                print("No card numbers found in relevant scraped transactions. Skipping matching logic.")
            return {'player_id': player_id, 'has_unmatched_cards': False}

        # --- PASS 1: WITHDRAWAL MATCHING ---
        if not self.summary_only:
            print("\n--- Pass 1: Withdrawal Matching ---")
        scraped_withdrawals = withdrawal_trans
        if not scraped_withdrawals:
            if not self.summary_only:
                print("No scraped withdrawal transactions found to process in Pass 1.")
        else:
            if not self.summary_only:
                print(f"Found {len(scraped_withdrawals)} scraped withdrawals to analyze.")
        
        # Stage 1: Gather all possible withdrawal matches
        withdrawal_matches = []
        used_platform_transaction_ids_pass1 = set()

        if scraped_withdrawals:
            for st in scraped_withdrawals:
                for pt in platform_transactions:
                    if pt['id'] in used_platform_transaction_ids_pass1:
                        continue
                    if self._is_match(st, pt):
                        casino_id = pt.get('Casino')
                        card_number = st.get('Card Number')
                        if casino_id and card_number:
                            withdrawal_matches.append({
                                'card_number': card_number,
                                'casino_id': casino_id,
                                'st_id': st.get('id'),
                                'pt_id': pt.get('id')
                            })
                            used_platform_transaction_ids_pass1.add(pt['id'])
                            break
        
        if not self.summary_only:
            print(f"Found {len(withdrawal_matches)} total possible withdrawal matches.")

        # Stage 2 & 3: Tally votes and determine winners for withdrawals
        final_assignments = {}
        assigned_card_numbers = set()
        if withdrawal_matches:
            card_casino_counts = {}
            for match in withdrawal_matches:
                card = match['card_number']
                casino = match['casino_id']
                if card not in card_casino_counts:
                    card_casino_counts[card] = {}
                if casino not in card_casino_counts[card]:
                    card_casino_counts[card][casino] = 0
                card_casino_counts[card][casino] += 1
            
            if not self.summary_only:
                print(f"Withdrawal vote counts: {card_casino_counts}")

            for card, casino_votes in card_casino_counts.items():
                if not casino_votes:
                    continue
                
                sorted_votes = sorted(casino_votes.items(), key=lambda item: item[1], reverse=True)
                
                if len(sorted_votes) > 1 and sorted_votes[0][1] == sorted_votes[1][1]:
                    if not self.summary_only:
                        print(f"Ambiguous withdrawal result for Card {card}: Tied vote. Deferring to deposit pass.")
                else:
                    winner_casino_id = sorted_votes[0][0]
                    final_assignments[card] = winner_casino_id
                    assigned_card_numbers.add(card)
                    if not self.summary_only:
                        print(f"Card {card} definitively matched with Casino ID {winner_casino_id} via withdrawals.")

        if not self.summary_only:
            print(f"Assigned {len(final_assignments)} cards after withdrawal pass.")

        # --- PASS 2: DEPOSIT MATCHING (for remaining cards) ---
        if not self.summary_only:
            print("\n--- Pass 2: Deposit Matching ---")
        
        used_platform_transaction_ids_pass2 = set()
        unassigned_card_numbers = all_card_numbers - assigned_card_numbers
        
        remaining_scraped_trans_pass2 = [
            st for st in deposit_trans 
            if st.get('Card Number') in unassigned_card_numbers
        ]
        
        scraped_deposits = remaining_scraped_trans_pass2
        
        if not scraped_deposits:
            if not self.summary_only:
                print("No remaining scraped deposit transactions to process in Pass 2.")
        else:
            if not self.summary_only:
                print(f"Found {len(scraped_deposits)} scraped deposits to analyze for unassigned cards.")
            
            remaining_platform_trans = [
                pt for pt in platform_transactions if pt['id'] not in used_platform_transaction_ids_pass1
            ]
            
            deposit_matches = []
            for st in scraped_deposits:
                for pt in remaining_platform_trans:
                    if pt['id'] in used_platform_transaction_ids_pass2:
                        continue
                    if self._is_deposit_match(st, pt):
                        casino_id = pt.get('Casino')
                        card_number = st.get('Card Number')
                        if casino_id and card_number:
                            deposit_matches.append({
                                'card_number': card_number,
                                'casino_id': casino_id,
                                'st_id': st.get('id'),
                                'pt_id': pt.get('id')
                            })
                            used_platform_transaction_ids_pass2.add(pt['id'])
                            break
            
            if not self.summary_only:
                print(f"Found {len(deposit_matches)} total possible deposit matches.")

            if deposit_matches:
                card_casino_counts_pass2 = {}
                for match in deposit_matches:
                    card = match['card_number']
                    casino = match['casino_id']
                    if card not in card_casino_counts_pass2:
                        card_casino_counts_pass2[card] = {}
                    if casino not in card_casino_counts_pass2[card]:
                        card_casino_counts_pass2[card][casino] = 0
                    card_casino_counts_pass2[card][casino] += 1
                
                if not self.summary_only:
                    print(f"Deposit vote counts: {card_casino_counts_pass2}")
                
                for card, casino_votes in card_casino_counts_pass2.items():
                    if not casino_votes:
                        continue
                    
                    sorted_votes = sorted(casino_votes.items(), key=lambda item: item[1], reverse=True)
                    
                    if len(sorted_votes) > 1 and sorted_votes[0][1] == sorted_votes[1][1]:
                        if not self.summary_only:
                            print(f"Ambiguous deposit result for Card {card}: Tied vote. Not assigning.")
                    else:
                        winner_casino_id = sorted_votes[0][0]
                        final_assignments[card] = winner_casino_id
                        assigned_card_numbers.add(card) # Track cards assigned in this pass
                        if not self.summary_only:
                            print(f"Card {card} definitively matched with Casino ID {winner_casino_id} via deposits.")
        
        # --- PASS 3: CORRECTION MATCHING (for remaining cards) ---
        if not self.summary_only:
            print("\n--- Pass 3: Correction Matching ---")

        unassigned_card_numbers = all_card_numbers - assigned_card_numbers
        
        remaining_scraped_trans_pass3 = [
            st for st in scraped_transactions 
            if st.get('Card Number') in unassigned_card_numbers
        ]
        
        # We are looking for scraped transactions that look like money coming IN to PlayPlus
        scraped_for_correction = [st for st in remaining_scraped_trans_pass3 if self._is_withdrawal(st)]

        if not scraped_for_correction:
            if not self.summary_only:
                print("No remaining scraped transactions to process in Pass 3.")
        else:
            if not self.summary_only:
                print(f"Found {len(scraped_for_correction)} scraped transactions to analyze for correction.")

            used_in_prev_passes = used_platform_transaction_ids_pass1.union(used_platform_transaction_ids_pass2)
            remaining_platform_trans = [
                pt for pt in platform_transactions if pt['id'] not in used_in_prev_passes
            ]
            
            correction_matches = []
            used_platform_transaction_ids_pass3 = set()
            for st in scraped_for_correction:
                for pt in remaining_platform_trans:
                    if pt['id'] in used_platform_transaction_ids_pass3:
                        continue
                    if self._is_correction_match(st, pt):
                        casino_id = pt.get('Casino')
                        card_number = st.get('Card Number')
                        if casino_id and card_number:
                            correction_matches.append({
                                'card_number': card_number,
                                'casino_id': casino_id,
                                'st_id': st.get('id'),
                                'pt_id': pt.get('id')
                            })
                            used_platform_transaction_ids_pass3.add(pt['id'])
                            break
            
            if not self.summary_only:
                print(f"Found {len(correction_matches)} total possible correction matches.")

            if correction_matches:
                card_casino_counts_pass3 = {}
                for match in correction_matches:
                    card = match['card_number']
                    casino = match['casino_id']
                    if card not in card_casino_counts_pass3:
                        card_casino_counts_pass3[card] = {}
                    if casino not in card_casino_counts_pass3[card]:
                        card_casino_counts_pass3[card][casino] = 0
                    card_casino_counts_pass3[card][casino] += 1
                
                if not self.summary_only:
                    print(f"Correction vote counts: {card_casino_counts_pass3}")
                
                for card, casino_votes in card_casino_counts_pass3.items():
                    if not casino_votes:
                        continue
                    
                    sorted_votes = sorted(casino_votes.items(), key=lambda item: item[1], reverse=True)
                    
                    if len(sorted_votes) > 1 and sorted_votes[0][1] == sorted_votes[1][1]:
                        if not self.summary_only:
                            print(f"Ambiguous correction result for Card {card}: Tied vote. Not assigning.")
                    else:
                        winner_casino_id = sorted_votes[0][0]
                        final_assignments[card] = winner_casino_id
                        if not self.summary_only:
                            print(f"Card {card} definitively matched with Casino ID {winner_casino_id} via correction.")
                        
                        # Update the incorrect platform transactions
                        if playplus_account_id:
                            for match in correction_matches:
                                if match['card_number'] == card and match['casino_id'] == winner_casino_id:
                                    pt_id_to_update = match['pt_id']
                                    # This payload assumes the API field to link a 'to' account is `to_bankaccount_id`
                                    update_payload = {'To_Account': playplus_account_id}
                                    self.update_platform_transaction(pt_id_to_update, update_payload, dry_run)

        # --- PASS 4: CARD UPGRADE MATCHING ---
        if not self.summary_only:
            print("\n--- Pass 4: Card Upgrade Matching ---")

        if not card_upgrade_pairs:
            if not self.summary_only:
                print("No card upgrade pairs found to process in Pass 4.")
        else:
            if not self.summary_only:
                print(f"Found {len(card_upgrade_pairs)} card upgrade pairs to analyze.")
            
            upgrades_processed = 0
            # Loop to handle chained upgrades (e.g., A->B, B->C)
            for _ in range(len(card_upgrade_pairs)):
                newly_assigned_in_pass = False
                for from_card, to_card in card_upgrade_pairs:
                    from_card_assigned = from_card in final_assignments
                    to_card_assigned = to_card in final_assignments

                    if from_card_assigned and not to_card_assigned:
                        casino_id = final_assignments[from_card]
                        final_assignments[to_card] = casino_id
                        assigned_card_numbers.add(to_card)
                        upgrades_processed += 1
                        newly_assigned_in_pass = True
                        if not self.summary_only:
                            print(f"Card Upgrade: Linked unassigned card {to_card} to Casino ID {casino_id} (via assigned card {from_card}).")
                    elif not from_card_assigned and to_card_assigned:
                        casino_id = final_assignments[to_card]
                        final_assignments[from_card] = casino_id
                        assigned_card_numbers.add(from_card)
                        upgrades_processed += 1
                        newly_assigned_in_pass = True
                        if not self.summary_only:
                            print(f"Card Upgrade: Linked unassigned card {from_card} to Casino ID {casino_id} (via assigned card {to_card}).")
                    elif from_card_assigned and to_card_assigned:
                        if final_assignments[from_card] != final_assignments[to_card]:
                            if not self.summary_only:
                                print(f"Warning: Card Upgrade conflict for cards {from_card} and {to_card}. They are assigned to different casinos: {final_assignments[from_card]} and {final_assignments[to_card]}.")
                if not newly_assigned_in_pass:
                    break # No new assignments in a full loop, stable state reached.
            
            if not self.summary_only:
                print(f"Processed {upgrades_processed} card assignments via upgrades.")

        if not self.summary_only:
            print(f"\nTotal assigned cards after all passes: {len(final_assignments)}")

        # Stage 4: Group updates by casino and perform them
        updates_by_casino = {}
        for card_number, casino_id in final_assignments.items():
            if casino_id not in updates_by_casino:
                updates_by_casino[casino_id] = []
            updates_by_casino[casino_id].append(card_number)

        if not self.summary_only and updates_by_casino:
            print("\n--- Proposed Updates ---")
        for casino_id, new_cards_to_add in updates_by_casino.items():
            target_account = next((acc for acc in casino_accounts if acc.get('casino_id') == casino_id), None)
            
            if not target_account:
                if not self.summary_only:
                    print(f"Could not find a casino account for casino_id {casino_id} to link cards: {', '.join(new_cards_to_add)}.")
                continue

            casino_name = target_account.get('casino_name', f"ID {casino_id}")
            if not self.summary_only:
                print(f"Proposing update for {casino_name}: Add cards {', '.join(new_cards_to_add)}.")
            
            existing_cards = target_account.get('play_plus_card_number', [])
            if isinstance(existing_cards, str) and existing_cards:
                existing_cards = [c.strip() for c in existing_cards.split(',')]
            elif not isinstance(existing_cards, list): # Handle None or other types
                existing_cards = []

            cards_were_added = False
            unique_new_cards = []
            for card in new_cards_to_add:
                if card not in existing_cards:
                    unique_new_cards.append(card)
                    cards_were_added = True
            
            if cards_were_added:
                updated_card_list = existing_cards + unique_new_cards
                update_data = {'play_plus_card_number': updated_card_list}
                self.update_casino_account(target_account['id'], update_data, dry_run)

                # Link the scraped transactions to the casino account
                if not self.summary_only:
                    print(f"Updating scraped transactions for casino account {target_account['id']} with newly linked cards...")
                
                casino_account_id = target_account['id']
                
                for card in unique_new_cards:
                    transactions_to_update = [st for st in scraped_transactions if st.get('Card Number') == card]
                    
                    for st in transactions_to_update:
                        st_id = st.get('id')
                        if st_id:
                            update_payload = {'casinoaccounts_id': casino_account_id}
                            self.update_scraped_transaction(st_id, update_payload, dry_run)
            else:
                if not self.summary_only:
                    print(f"All proposed cards for {casino_name} ({', '.join(new_cards_to_add)}) are already linked.")
        
        if not self.summary_only:
            print(f"\nFinished processing player ID: {player_id}")

        assigned_cards_in_run = set(final_assignments.keys())
        has_unmatched = bool(all_card_numbers - assigned_cards_in_run)

        if has_unmatched:
            unmatched_list = sorted(list(all_card_numbers - assigned_cards_in_run))
            print(f"Warning: Player {player_id} has unmatched card numbers: {unmatched_list}")
            
        return {'player_id': player_id, 'has_unmatched_cards': has_unmatched}


    def run(self, player_id=None, player_stages=None, dry_run=False):
        """
        Main execution function.
        """
        player_ids = self.get_player_ids(player_id, player_stages)
        if not player_ids:
            print("No players to process.")
            return

        print(f"Starting analysis for {len(player_ids)} players...")

        unmatched_player_ids = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(self.process_player, pid, dry_run): pid for pid in player_ids}

            for i, future in enumerate(as_completed(futures)):
                pid = futures[future]
                try:
                    result = future.result()
                    if result and result.get('has_unmatched_cards'):
                        unmatched_player_ids.append(pid)
                except Exception as e:
                    print(f"An error occurred while processing player {pid}: {e}")
                finally:
                    if not self.summary_only:
                        print(f"--- Completed processing player {i + 1}/{len(player_ids)} ---")

        print("\n" + "="*60)
        print("--- BATCH SUMMARY ---")
        print("="*60)

        if unmatched_player_ids:
            print("\nPlayers with unmatched card numbers:")
            print("The following players had Play Plus transactions but no definitive match could be found for at least one card.")
            print(', '.join(map(str, sorted(unmatched_player_ids))))
        else:
            print("\nAll players with relevant transactions were successfully matched.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Play Plus Casino Account Matching Script")
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

    matcher = PlayPlusMatcher(summary_only=args.summary_only)
    matcher.run(player_id=args.player_id, player_stages=player_stages_to_run, dry_run=args.dry_run)

