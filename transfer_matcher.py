import requests
import os
import re
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any
from dataclasses import dataclass
from config import PLATFORM_TRANSACTIONS_API_URL, BANK_TRANSACTIONS_API_URL

# --- Constants ---
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
BETTING_PAYPAL_KEYWORDS = [
    "money transfer from",
]

# --- Pre-compiled Regex for Performance ---
COMPILED_BANK_TRANSFER_KEYWORDS = [re.compile(k) for k in BETTING_BANK_TRANSFER_KEYWORDS]
COMPILED_PAYPAL_KEYWORDS = [re.compile(k) for k in BETTING_PAYPAL_KEYWORDS]

# --- Data Classes for Structure ---
@dataclass
class SimpleMatch:
    """Data class for a simple one-to-one matched transaction pair"""
    platform_transaction: Dict[str, Any]
    bank_transaction: Dict[str, Any]
    match_date: str

@dataclass
class UnmatchedTransactionInfo:
    """Data class for an unmatched transaction with a reason"""
    platform_transaction: Dict[str, Any]
    reason: str

@dataclass
class MatchResults:
    """Data class for complete matching results"""
    simple_matches: List[SimpleMatch]
    unmatched_platform_transactions: List[UnmatchedTransactionInfo]
    
# --- Main Matcher Class ---
class TransferTransactionMatcher:
    def __init__(self, platform_transactions: List[Dict[str, Any]], bank_transactions: List[Dict[str, Any]]):
        self.platform_transactions = platform_transactions
        self.bank_transactions = bank_transactions
        self._preprocess_data()

    def _preprocess_data(self):
        """Pre-process and filter data for matching."""
        # Pre-process and filter platform transactions in a single pass
        valid_pts = []
        for pt in self.platform_transactions:
            if not (pt.get('Transaction_Type', '').lower() == 'transfer' and
                    len(pt.get('related_bank_transaction', [])) < 2 and
                    pt.get('from', {}).get('Account_Type', '').lower() == 'betting bank account' and
                    pt.get('to', {}).get('Account_Type', '').lower() == 'betting paypal account'):
                continue
            
            try:
                pt['parsed_date'] = datetime.strptime(pt['Date'], '%Y-%m-%d')
                valid_pts.append(pt)
            except (ValueError, KeyError, TypeError):
                continue
        self.platform_transactions = valid_pts
        
        # Sort by date to process chronologically
        self.platform_transactions.sort(key=lambda t: t['parsed_date'])
        
        # Pre-process, filter, and group bank transactions in a single pass
        self.bank_transactions_by_account: Dict[Any, List[Dict[str, Any]]] = {}
        for bt in self.bank_transactions:
            if bt.get('linked_transaction'):
                continue

            try:
                bt['parsed_date'] = datetime.strptime(bt['date'], '%Y-%m-%d')
                account_id = bt.get('bankaccount_id')
                if account_id:
                    self.bank_transactions_by_account.setdefault(account_id, []).append(bt)
            except (ValueError, KeyError, TypeError):
                continue
        
        # The raw bank_transactions list is no longer needed after grouping
        self.bank_transactions = []

    def match_transactions(self) -> MatchResults:
        """
        Matches transfer platform transactions with their corresponding bank transactions.
        A transfer is considered matched if either the positive (credit), the negative (debit),
        or both legs of the transaction are found. A simple match is created for each
        bank transaction found.
        """
        simple_matches = []
        unmatched_info: List[UnmatchedTransactionInfo] = []
        matched_bt_ids = set()
        
        for pt in self.platform_transactions:
            print(f"\n--- Processing PT ID: {pt.get('id')} ---")
            positive_match_candidate = None
            negative_match_candidate = None
            
            try:
                transfer_date = pt['parsed_date']
                transfer_amount = pt.get('Amount')
                from_account = pt.get('from', {})
                from_bank_account_id = from_account.get('bankaccount_id')
                to_account = pt.get('to', {})
                to_bank_account_id = to_account.get('bankaccount_id')

                # Check which legs are already matched
                related_bts = pt.get('related_bank_transaction', [])
                print(f"  [Debug] Raw related_bts: {related_bts} (type: {type(related_bts)})")
                
                # Ensure related_bts is a list of dicts, not a string
                if isinstance(related_bts, str) and related_bts:
                    try:
                        related_bts = json.loads(related_bts)
                        print(f"  [Debug] Parsed related_bts: {related_bts}")
                    except json.JSONDecodeError:
                        related_bts = [] # If parsing fails, treat as no relations

                # Count how many related bank transactions belong to the 'from' and 'to' accounts
                from_account_count = 0
                to_account_count = 0
                
                for r_bt in related_bts:
                    # Check ONLY the new API structure for bank account ID
                    bankaccount_id_value = r_bt.get('bankaccount', {}).get('id')
                    
                    # Convert to strings for comparison
                    if bankaccount_id_value and str(bankaccount_id_value) == str(from_bank_account_id):
                        from_account_count += 1
                    
                    if bankaccount_id_value and str(bankaccount_id_value) == str(to_bank_account_id):
                        to_account_count += 1
                
                from_account_matched = from_account_count > 0
                to_account_matched = to_account_count > 0
                
                print(f"  [Debug] From account ({from_bank_account_id}) match count: {from_account_count}")
                print(f"  [Debug] To account ({to_bank_account_id}) match count: {to_account_count}")

                # --- Step 1: Find a potential "positive" match candidate (credit) ---
                # Only search if the from_account has zero matches (prevent duplicates from same account)
                if from_account_count == 0:
                    for bt in self.bank_transactions_by_account.get(from_bank_account_id, []):
                        if bt.get('id') in matched_bt_ids: continue
                        if not (bt.get('amount', 0) > 0): continue
                        
                        bank_txn_date = bt['parsed_date']
                        bank_txn_name = bt.get('name', '').lower()
                        
                        if (bt.get('amount') == transfer_amount and
                            abs((bank_txn_date - transfer_date).days) <= 7 and
                            any(regex.search(bank_txn_name) for regex in COMPILED_BANK_TRANSFER_KEYWORDS)):
                            positive_match_candidate = bt
                            print(f"  [Debug] Found positive candidate: BT ID {bt.get('id')}")
                            break
                
                # --- Step 2: Find a potential "negative" match candidate (debit) ---
                # Only search if the to_account has zero matches (prevent duplicates from same account)
                if to_account_count == 0:
                    for bt in self.bank_transactions_by_account.get(to_bank_account_id, []):
                        if bt.get('id') in matched_bt_ids: continue
                        if positive_match_candidate and bt.get('id') == positive_match_candidate.get('id'): continue
                        if not (bt.get('amount', 0) < 0): continue
                        
                        bank_txn_date = bt['parsed_date']
                        bank_txn_name = bt.get('name', '').lower()
                        
                        if (bt.get('amount') == -transfer_amount and
                            abs((bank_txn_date - transfer_date).days) <= 7 and
                            any(regex.search(bank_txn_name) for regex in COMPILED_PAYPAL_KEYWORDS)):
                            negative_match_candidate = bt
                            print(f"  [Debug] Found negative candidate: BT ID {bt.get('id')}")
                            break

            except (ValueError, KeyError) as e:
                print(f"Data error during match search for PT ID {pt.get('Transaction_ID')}: {e}")
                continue

            # --- Step 3: Validate and Create Matches ---
            # CRITICAL: Prevent matching two bank transactions from the same bank account ID
            # We should only match:
            # - One from 'from_account' (bank account ID 2969) and one from 'to_account' (bank account ID 2970)
            # - OR just one from either side
            # - NEVER two from the same side
            if (positive_match_candidate and negative_match_candidate and
                    positive_match_candidate.get('bankaccount_id') == negative_match_candidate.get('bankaccount_id')):
                print(f"  [Debug] Validation FAIL: Both candidates from same bankaccount_id ({positive_match_candidate.get('bankaccount_id')}). Discarding negative match, keeping positive.")
                negative_match_candidate = None  # Discard the negative candidate

            is_matched = False
            if positive_match_candidate:
                simple_matches.append(SimpleMatch(
                    platform_transaction=pt,
                    bank_transaction=positive_match_candidate,
                    match_date=datetime.now().isoformat()
                ))
                matched_bt_ids.add(positive_match_candidate.get('id'))
                is_matched = True
            
            if negative_match_candidate:
                simple_matches.append(SimpleMatch(
                    platform_transaction=pt,
                    bank_transaction=negative_match_candidate,
                    match_date=datetime.now().isoformat()
                ))
                matched_bt_ids.add(negative_match_candidate.get('id'))
                is_matched = True

            if not is_matched:
                # Only include in unmatched analysis if there are NO existing related bank transactions
                # (i.e., completely unmatched, not partially matched)
                if from_account_count == 0 and to_account_count == 0:
                    # Post-mortem analysis for why the transaction was not matched
                    reasons = []
                    try:
                        transfer_amount = pt.get('Amount')
                        transfer_date = pt['parsed_date']
                        from_bank_account_id = pt.get('from', {}).get('bankaccount_id')
                        to_bank_account_id = pt.get('to', {}).get('bankaccount_id')

                        # --- Analyze "FROM" leg ---
                        from_candidates = self.bank_transactions_by_account.get(from_bank_account_id, [])
                        if not from_candidates:
                            reasons.append(f"FROM leg: No BTs found for account ID {from_bank_account_id}.")
                        else:
                            amount_matches = [bt for bt in from_candidates if bt.get('amount') == transfer_amount]
                            if not amount_matches:
                                reasons.append(f"FROM leg: No BT with amount {transfer_amount} found for account ID {from_bank_account_id}.")
                            else:
                                date_matches = [bt for bt in amount_matches if abs((bt['parsed_date'] - transfer_date).days) <= 7]
                                if not date_matches:
                                    dates = sorted([bt['date'] for bt in amount_matches])
                                    reasons.append(f"FROM leg: Found BT with correct amount, but date was outside 7-day window. Candidate dates: {dates}")
                                else:
                                    keyword_matches = [bt for bt in date_matches if any(regex.search(bt.get('name', '').lower()) for regex in COMPILED_BANK_TRANSFER_KEYWORDS)]
                                    if not keyword_matches:
                                        names = [bt['name'] for bt in date_matches]
                                        reasons.append(f"FROM leg: Found BT with correct amount/date, but description did not match keywords. Candidate names: {names}")
                        
                        # --- Analyze "TO" leg ---
                        to_candidates = self.bank_transactions_by_account.get(to_bank_account_id, [])
                        if not to_candidates:
                            reasons.append(f"TO leg: No BTs found for account ID {to_bank_account_id}.")
                        else:
                            amount_matches = [bt for bt in to_candidates if bt.get('amount') == -transfer_amount]
                            if not amount_matches:
                                reasons.append(f"TO leg: No BT with amount {-transfer_amount} found for account ID {to_bank_account_id}.")
                            else:
                                date_matches = [bt for bt in amount_matches if abs((bt['parsed_date'] - transfer_date).days) <= 7]
                                if not date_matches:
                                    dates = sorted([bt['date'] for bt in amount_matches])
                                    reasons.append(f"TO leg: Found BT with correct amount, but date was outside 7-day window. Candidate dates: {dates}")
                                else:
                                    keyword_matches = [bt for bt in date_matches if any(regex.search(bt.get('name', '').lower()) for regex in COMPILED_PAYPAL_KEYWORDS)]
                                    if not keyword_matches:
                                        names = [bt['name'] for bt in date_matches]
                                        reasons.append(f"TO leg: Found BT with correct amount/date, but description did not match keywords. Candidate names: {names}")

                    except (ValueError, KeyError) as e:
                        reasons.append(f"An error occurred during analysis: {e}")

                    unmatched_info.append(UnmatchedTransactionInfo(
                        platform_transaction=pt,
                        reason="\n".join(reasons) if reasons else "No potential bank transaction candidates found."
                    ))

        return MatchResults(
            simple_matches=simple_matches,
            unmatched_platform_transactions=unmatched_info
        )

# --- Helper Functions for Data Fetching ---
def get_all_platform_transactions(player_id: int) -> List[Dict[str, Any]]:
    """Fetches all platform transactions for a given player."""
    try:
        params = {'user_id': player_id, 'start_date': None, 'end_date': None}
        response = requests.get(PLATFORM_TRANSACTIONS_API_URL, params=params)
        response.raise_for_status()
        return response.json()
    except (requests.RequestException, ValueError) as e:
        print(f"Error fetching platform transactions: {e}")
        return []

def get_all_bank_transactions(player_id: int) -> List[Dict[str, Any]]:
    """Fetches all bank transactions for a given player, sorted by date."""
    try:
        params = {'player_id': player_id, 'start_date': None, 'end_date': None}
        response = requests.get(BANK_TRANSACTIONS_API_URL, params=params)
        response.raise_for_status()
        bank_transactions = response.json().get('bankTransactions', [])
        
        # Filter out transactions that are missing a date to ensure stable sorting
        valid_transactions = [t for t in bank_transactions if t.get('date')]
        
        # Sort by date in ascending order
        valid_transactions.sort(key=lambda t: datetime.strptime(t.get('date'), '%Y-%m-%d'))
        
        return valid_transactions
    except (requests.RequestException, ValueError) as e:
        print(f"Error fetching bank transactions: {e}")
        return []
