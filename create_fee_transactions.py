import requests
import re
from config import (
    PLAYERS_API_URL,
    BANK_TRANSACTIONS_API_URL,
    USER_ACCOUNTS_API_URL,
    CREATE_PLATFORM_TRANSACTION_API_URL,
    UPDATE_BANK_TRANSACTIONS_API_URL,
    AI_USER_ID,
    FEES_ACCOUNT_ID,
    API_TIMEOUT
)

# Keyword lists adapted from fee_matcher.py
FEES_TO_BETTING_BANK_KEYWORDS = [
    'MONTHLY SERVICE FEE', 'OVERDRAFT', r'CHECKBOOK,?\s*INC', 'PAYPAL ACCTVERIFY',
    'SIGHTLINE_SUTTON', 'HSAWCSPCUSTODIAN ACCTVERIFY'
]
BETTING_BANK_TO_FEES_KEYWORDS = [
    r'Checkbook,?\s*Inc\sMICRO\sDEP', r'CHECKBOOK,?\s*INC\sACCTVERIFY', 'PAYPAL ACCTVERIFY',
    'SIGHTLINE_BNKGEO ACCOUNTREG', 'SIGHTLINE_SUTTON', 'HSAWCSPCUSTODIAN ACCTVERIFY'
]

class FeeTransactionCreator:
    """
    Identifies unmatched bank transactions that represent fees, creates the
    corresponding platform transaction, and links them.
    """

    def _fetch_data(self, url, params=None):
        """Generic function to fetch data from an API endpoint."""
        try:
            response = requests.get(url, params=params, timeout=API_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data from {url}: {e}")
            return None

    def _get_user_account_mapping(self, player_id):
        """Fetches a user's accounts and creates a mapping from bankaccount_id to account_id."""
        mapping = {}
        accounts = self._fetch_data(USER_ACCOUNTS_API_URL, params={'user_id': player_id})
        if accounts:
            for acc in accounts:
                if acc.get('bankaccount_id'):
                    mapping[acc['bankaccount_id']] = acc.get('id')
        return mapping

    def _create_platform_transaction(self, payload):
        """Creates a new platform transaction."""
        try:
            response = requests.post(CREATE_PLATFORM_TRANSACTION_API_URL, json=payload, timeout=API_TIMEOUT)
            response.raise_for_status()
            print(f"Successfully created PT for BT {payload.get('original_bt_id', 'N/A')}")
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Failed to create platform transaction: {e}")
            return None

    def _link_bank_transaction(self, platform_transaction_id, bank_transaction):
        """Links a bank transaction to a newly created platform transaction."""
        bt_api_id = bank_transaction.get('transaction_id')
        if not bt_api_id:
            print(f"ERROR: Bank transaction ID {bank_transaction.get('id')} is missing 'transaction_id'.")
            return
        
        url = UPDATE_BANK_TRANSACTIONS_API_URL.format(transaction_id=bt_api_id)
        payload = {"transaction_link": platform_transaction_id, "last_edited_by": AI_USER_ID}
        try:
            response = requests.patch(url, json=payload, timeout=API_TIMEOUT)
            response.raise_for_status()
            print(f"Successfully linked BT {bt_api_id} to new PT {platform_transaction_id}")
        except requests.exceptions.RequestException as e:
            print(f"Failed to link BT {bt_api_id}: {e}")

    def _log_dry_run_action(self, bank_transaction, payload):
        """Logs the action that would be taken in a dry run."""
        print("\n" + "="*50)
        print(f"[DRY RUN] Would create and link for BT ID: {bank_transaction.get('id')}")
        print(f"  - Bank Transaction Details: Date: {bank_transaction.get('date')}, Amount: {bank_transaction.get('amount')}, Name: '{bank_transaction.get('name')}'")
        print(f"  - Platform Transaction to Create:")
        for key, value in payload.items():
            print(f"    - {key}: {value}")
        print("="*50)

    def process_players(self, dry_run=False, player_id=None, player_stage=None):
        """Main processing loop for players."""
        print(f"Starting reverse fee matching process... (Mode: {'Dry Run' if dry_run else 'Live'})")

        players_to_process = []
        if player_id:
            players_to_process.append({'id': player_id})
        else:
            all_players = self._fetch_data(PLAYERS_API_URL) or []
            if player_stage:
                players_to_process = [p for p in all_players if p.get('player_stage') == player_stage]
                print(f"Filtered to {len(players_to_process)} players in stage: '{player_stage}'")
            else:
                players_to_process = all_players
        
        if not players_to_process:
            print("No players found to process.")
            return

        total_created = 0
        for player in players_to_process:
            current_player_id = player.get('id')
            print(f"\n--- Processing Player ID: {current_player_id} ---")

            account_mapping = self._get_user_account_mapping(current_player_id)
            if not account_mapping:
                print(f"Warning: Could not get account mapping for player {current_player_id}. Skipping.")
                continue

            bank_transactions = self._fetch_data(BANK_TRANSACTIONS_API_URL, params={'player_id': current_player_id})
            unmatched_bts = [bt for bt in bank_transactions.get('bankTransactions', []) if not bt.get('linked_transaction')]

            if not unmatched_bts:
                print("No unmatched bank transactions found.")
                continue
            
            for bt in unmatched_bts:
                try:
                    amount = float(bt.get('amount', 0))
                    if not (0 < abs(amount) <= 2.00):
                        continue
                    
                    description = f"{bt.get('name', '')} {bt.get('counterparty_name', '')}"
                    from_account, to_account, matched_keywords = None, None, None

                    if amount > 0: # Positive amount: Betting Bank -> Fees
                        if any(re.search(kw, description, re.IGNORECASE) for kw in BETTING_BANK_TO_FEES_KEYWORDS):
                            from_account = account_mapping.get(bt.get('bankaccount_id'))
                            to_account = FEES_ACCOUNT_ID
                            matched_keywords = BETTING_BANK_TO_FEES_KEYWORDS
                    else: # Negative amount: Fees -> Betting Bank
                        if any(re.search(kw, description, re.IGNORECASE) for kw in FEES_TO_BETTING_BANK_KEYWORDS):
                            from_account = FEES_ACCOUNT_ID
                            to_account = account_mapping.get(bt.get('bankaccount_id'))
                            matched_keywords = FEES_TO_BETTING_BANK_KEYWORDS

                    if from_account and to_account:
                        payload = {
                            "From_Account": from_account,
                            "To_Account": to_account,
                            "Transaction_Type": "fees",
                            "Amount": abs(amount),
                            "Date": bt.get('date'),
                            "User_ID": current_player_id,
                            "Added_By": AI_USER_ID,
                            "Status": "Completed",
                            "original_bt_id": bt.get('id') # For logging
                        }

                        if dry_run:
                            self._log_dry_run_action(bt, payload)
                        else:
                            new_pt = self._create_platform_transaction(payload)
                            if new_pt and new_pt.get('id'):
                                self._link_bank_transaction(new_pt.get('id'), bt)
                        total_created += 1

                except (ValueError, TypeError) as e:
                    print(f"Skipping BT {bt.get('id')} due to data error: {e}")
        
        print(f"\n--- Process Complete ---")
        print(f"Total new platform transactions created and linked: {total_created}")
        if dry_run:
            print("Note: This was a dry run. No changes were made.")
