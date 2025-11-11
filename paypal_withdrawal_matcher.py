import requests
from datetime import datetime, timedelta
import json
from config import (
    PLAYERS_API_URL,
    PLATFORM_TRANSACTIONS_API_URL,
    SCRAPED_TRANSACTION_API_URL,
    USER_ACCOUNTS_API_URL,
    BANK_TRANSACTIONS_API_URL,
    UPDATE_PLATFORM_TRANSACTIONS_API_URL,
    UPDATE_BANK_TRANSACTIONS_API_URL,
    AI_USER_ID,
    API_TIMEOUT
)

class PaypalWithdrawalMatcher:
    """
    A second-pass matcher to find and correct withdrawal transactions that
    should have been directed to a user's Betting PayPal account.
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
                    if name and keywords:
                        keyword_map[name] = [k.strip().upper() for k in keywords.split(',') if k.strip()]
        except (FileNotFoundError, json.JSONDecodeError) as e:
            print(f"Error loading casino keywords: {e}")
        return keyword_map

    def _fetch_data(self, url, params=None):
        """Generic function to fetch data from an API endpoint using GET."""
        try:
            response = requests.get(url, params=params, timeout=API_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data from {url}: {e}")
            return None

    def _update_platform_transaction(self, pt_id, new_to_account_id):
        """Updates the To_Account of a platform transaction."""
        url = UPDATE_PLATFORM_TRANSACTIONS_API_URL.format(platform_transaction_id=pt_id)
        payload = {"To_Account": new_to_account_id, "last_edited_by": AI_USER_ID}
        try:
            response = requests.patch(url, json=payload, timeout=API_TIMEOUT)
            response.raise_for_status()
            print(f"Successfully updated To_Account for PT {pt_id} to {new_to_account_id}")
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Failed to update PT {pt_id}: {e}")
            return None

    def _link_bank_transaction(self, pt_id, bank_transaction):
        """Links a bank transaction to a platform transaction."""
        bt_api_id = bank_transaction.get('transaction_id')
        if not bt_api_id: return
        url = UPDATE_BANK_TRANSACTIONS_API_URL.format(transaction_id=bt_api_id)
        payload = {"transaction_link": pt_id, "last_edited_by": AI_USER_ID}
        try:
            response = requests.patch(url, json=payload, timeout=API_TIMEOUT)
            response.raise_for_status()
            print(f"Successfully linked BT {bt_api_id} to PT {pt_id}")
        except requests.exceptions.RequestException as e:
            print(f"Failed to link BT {bt_api_id}: {e}")

    def _log_dry_run_action(self, pt, bt, paypal_account):
        """Logs the actions that would be taken in a dry run."""
        print("\n" + "="*50)
        print(f"[DRY RUN] Found a match for PT ID: {pt.get('id')}")
        print(f"  - Action 1: Would update PT {pt.get('id')}'s To_Account to {paypal_account.get('id')}")
        print(f"  - Action 2: Would link PT {pt.get('id')} to BT {bt.get('id')}")
        print(f"  - Details:")
        print(f"    - PT: Date={pt.get('Date')}, Amount={pt.get('Amount')}, Name='{pt.get('Name')}'")
        print(f"    - BT: Date={bt.get('date')}, Amount={bt.get('amount')}, Name='{bt.get('name')}'")
        print("="*50)

    def process_player(self, player_id, dry_run=False):
        """Main processing logic for a single player."""
        print(f"\n--- Processing Player ID: {player_id} ---")

        # 1. Get default PayPal account
        accounts = self._fetch_data(USER_ACCOUNTS_API_URL, {'user_id': player_id}) or []
        default_paypal = next((acc for acc in accounts if acc.get('Account_Type') == 'Betting PayPal account' and acc.get('isDefault')), None)
        if not default_paypal or not default_paypal.get('bankaccount_id'):
            print("No default Betting PayPal account found. Skipping.")
            return 0
        
        # 2. Get data
        unmatched_pts_unfiltered = [pt for pt in (self._fetch_data(PLATFORM_TRANSACTIONS_API_URL, {'user_id': player_id}) or []) if pt.get('Transaction_Type', '').lower() == 'withdrawal' and not pt.get('related_bank_transaction')]
        
        # Early exit if there are no withdrawals to process
        if not unmatched_pts_unfiltered:
            print("No unmatched withdrawals found to process. Skipping.")
            return 0

        # Add a filter to skip recent transactions
        cutoff_date = datetime.now() - timedelta(days=15)
        unmatched_pts = []
        for pt in unmatched_pts_unfiltered:
            try:
                if datetime.strptime(pt.get('Date'), '%Y-%m-%d') < cutoff_date:
                    unmatched_pts.append(pt)
            except (ValueError, TypeError):
                continue # Skip transactions with invalid date formats
        
        print(f"Found {len(unmatched_pts_unfiltered)} total unmatched withdrawals. After filtering for transactions older than 15 days, {len(unmatched_pts)} remain.")

        scraped_txs = [st for st in (self._fetch_data(SCRAPED_TRANSACTION_API_URL, {'user_id': player_id}) or []) if st.get('Source') == 'paypal' and st.get('Type') == 'transfer_received']
        unmatched_bts = [bt for bt in (self._fetch_data(BANK_TRANSACTIONS_API_URL, {'player_id': player_id}).get('bankTransactions', []) or []) if not bt.get('linked_transaction') and bt.get('bankaccount_id') == default_paypal['bankaccount_id']]
        
        if not all([unmatched_pts, scraped_txs, unmatched_bts]):
            print("Missing necessary data (unmatched PTs, scraped TXs, or unmatched BTs). Skipping.")
            return 0

        # Sort for deterministic matching
        unmatched_pts.sort(key=lambda x: x.get('Date', ''))
        unmatched_bts.sort(key=lambda x: x.get('date', ''))

        matches_found = 0
        for pt in unmatched_pts:
            try:
                pt_date = datetime.strptime(pt.get('Date'), '%Y-%m-%d')
                pt_amount = float(pt.get('Amount', 0))

                # Step 2: Primary Match (PT -> Scraped)
                matching_st = None
                for st in scraped_txs:
                    st_date = datetime.fromtimestamp(int(st.get('Transaction Time')) / 1000)
                    if abs(pt_date - st_date) <= timedelta(days=7) and abs(pt_amount - float(st.get('Net', 0))) < 0.01:
                        matching_st = st
                        break
                
                if not matching_st:
                    continue

                # Step 3: Secondary Match (PT -> BT)
                pt_casino_name = pt.get('Name', '').upper()
                keywords_for_casino = self.casino_keywords.get(pt_casino_name, [])
                if not keywords_for_casino:
                    continue

                found_match_bt = None
                for bt in unmatched_bts:
                    bt_date = datetime.strptime(bt.get('date'), '%Y-%m-%d')
                    description = f"{bt.get('name', '')} {bt.get('counterparty_name', '')}".upper()

                    if (round(float(bt.get('amount', 0)), 2) == -round(pt_amount, 2) and
                        abs(pt_date - bt_date) <= timedelta(days=9) and
                        any(kw in description for kw in keywords_for_casino)):
                        found_match_bt = bt
                        break
                
                if found_match_bt:
                    if dry_run:
                        self._log_dry_run_action(pt, found_match_bt, default_paypal)
                    else:
                        if self._update_platform_transaction(pt.get('id'), default_paypal.get('id')):
                            self._link_bank_transaction(pt.get('id'), found_match_bt)
                    
                    matches_found += 1
                    unmatched_bts.remove(found_match_bt) # Prevent re-use

            except (ValueError, TypeError) as e:
                print(f"Skipping PT {pt.get('id')} due to data error: {e}")
        
        return matches_found

    def run(self, dry_run=False, player_id=None, player_stages=None):
        """Entry point to run the matcher for all or a single player."""
        players_to_process = []
        if player_id:
            players_to_process = [{'id': player_id}]
        else:
            all_players = self._fetch_data(PLAYERS_API_URL)
            if all_players:
                if player_stages:
                    players_to_process = [p for p in all_players if p.get('player_stage') in player_stages]
                    print(f"Filtered to {len(players_to_process)} players in stages: {player_stages}")
                else:
                    players_to_process = all_players
        
        if not players_to_process:
            print("No players to process after filtering. Aborting.")
            return

        total_matches = 0
        for player in players_to_process:
            if player_id := player.get('id'):
                total_matches += self.process_player(player_id, dry_run)
        
        print(f"\n--- PayPal Reconciliation Complete ---")
        print(f"Total matches found and processed: {total_matches}")
        if dry_run:
            print("Note: This was a dry run. No changes were made.")
