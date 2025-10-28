"""
PayPal to Bank Transfer Matcher
Matches platform transfer transactions where funds move FROM Betting PayPal TO Betting Bank
with their corresponding bank transactions.

Based on analysis of 879 successfully matched transfers (linkedBPTransfers.csv)
"""

import requests
import os
import re
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any
from dataclasses import dataclass
import requests
import re
import json
from config import (
    PLATFORM_TRANSACTIONS_API_URL, BANK_TRANSACTIONS_API_URL, 
    SCRAPED_TRANSACTION_API_URL, CREATE_PLATFORM_TRANSACTION_API_URL, 
    UPDATE_PLATFORM_TRANSACTIONS_API_URL, UPDATE_BANK_TRANSACTIONS_API_URL,
    TRANSFER_ACCOUNT_ID, FEES_ACCOUNT_ID, AI_USER_ID
)

# --- Keywords (Data-Driven from CSV Analysis) ---
# PayPal side keywords: Matches 99.75% of FROM account transactions
BETTING_PAYPAL_KEYWORDS = [
    "money transfer to",  # Matches transactions like "Money Transfer to WELLS FARGO BANK"
]

# Bank side keywords: Matches 98.4% of TO account transactions
BETTING_BANK_TRANSFER_KEYWORDS = [
    "^paypal transfer$",           # Exact match: 19.12%
    "paypal transfer",             # Contains: 49.54%
    "transfer.*paypal",            # Either order: 77.76%
    "^paypal",                     # Starts with PayPal: 68.32%
    "rtp.*paypal",                 # Real-time payment: 10.94%
    "instant.*paypal",             # Instant payment: 5.07%
    "paypal paypal",
    "money transfer authorized"
]

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
class ThreeWayMatch:
    """Data class for a three-way matched transaction (PayPal with fees)"""
    original_platform_transaction: Dict[str, Any]  # The existing PT (Betting Bank -> Betting PayPal)
    paypal_bank_transaction: Dict[str, Any]        # The PayPal bank transaction with fees
    bank_side_transaction: Dict[str, Any]          # The bank-side transaction (net amount)
    scraped_transaction: Dict[str, Any]            # The scraped transaction for verification
    net_amount: float                              # The net amount after fees
    fee_amount: float                              # The fee amount
    match_date: str

@dataclass
class MatchResults:
    """Data class for complete matching results"""
    simple_matches: List[SimpleMatch]
    three_way_matches: List[ThreeWayMatch]
    unmatched_platform_transactions: List[UnmatchedTransactionInfo]
    
# --- Main Matcher Class ---
class PayPalToBankMatcher:
    def __init__(self, platform_transactions: List[Dict[str, Any]], bank_transactions: List[Dict[str, Any]], scraped_transactions: List[Dict[str, Any]] = None):
        self.platform_transactions = platform_transactions
        self.bank_transactions = bank_transactions
        self.scraped_transactions = scraped_transactions or []
        self._preprocess_data()

    def _preprocess_data(self):
        """Pre-process and filter data for matching."""
        # Filter for relevant platform transactions: Betting PayPal -> Betting Bank
        self.platform_transactions = [
            pt for pt in self.platform_transactions
            if (pt.get('Transaction_Type', '').lower() == 'transfer' and
                len(pt.get('related_bank_transaction', [])) < 2 and
                pt.get('from', {}).get('Account_Type', '').lower() == 'betting paypal account' and
                pt.get('to', {}).get('Account_Type', '').lower() == 'betting bank account')
        ]
        # Sort by date to process chronologically
        self.platform_transactions.sort(key=lambda t: datetime.strptime(t.get('Date'), '%Y-%m-%d'))
        
        # Filter for relevant bank transactions (only unlinked ones)
        self.bank_transactions = [
            bt for bt in self.bank_transactions if not bt.get('linked_transaction')
        ]

    def match_transactions(self) -> MatchResults:
        """
        Matches transfer platform transactions (PayPal -> Bank) with their corresponding bank transactions.
        A transfer is considered matched if either the PayPal side (positive), the Bank side (negative),
        or both legs of the transaction are found.
        Also handles three-way matching for PayPal transactions with fees.
        """
        simple_matches = []
        three_way_matches = []
        unmatched_info: List[UnmatchedTransactionInfo] = []
        unmatched_pts = self.platform_transactions[:]
        available_bts = self.bank_transactions[:]
        
        for pt in self.platform_transactions:
            print(f"\n--- Processing PT ID: {pt.get('id')} ---")
            paypal_side_match = None
            bank_side_match = None
            
            try:
                transfer_date = datetime.strptime(pt['Date'], '%Y-%m-%d')
                transfer_amount = pt.get('Amount')
                
                # Use the bankaccount_id from the nested 'from' and 'to' objects for matching
                from_bank_account_id = pt.get('from', {}).get('bankaccount_id')
                to_bank_account_id = pt.get('to', {}).get('bankaccount_id')
                
                # Get the rich details from the nested objects for logging/display
                from_account_details = pt.get('from', {})
                to_account_details = pt.get('to', {})


                # Check which legs are already matched
                related_bts = pt.get('related_bank_transaction', [])
                print(f"  [Debug] Raw related_bts: {related_bts} (type: {type(related_bts)})")
                
                # Ensure related_bts is a list of dicts, not a string
                if isinstance(related_bts, str) and related_bts:
                    try:
                        related_bts = json.loads(related_bts)
                        print(f"  [Debug] Parsed related_bts: {related_bts}")
                    except json.JSONDecodeError:
                        related_bts = []

                # Count how many related bank transactions belong to the 'from' and 'to' accounts
                from_account_count = 0  # PayPal side
                to_account_count = 0    # Bank side
                
                for r_bt in related_bts:
                    # Check the new API structure for bank account ID
                    bankaccount_id_value = r_bt.get('bankaccount', {}).get('id')
                    
                    # Convert to strings for comparison
                    if bankaccount_id_value and str(bankaccount_id_value) == str(from_bank_account_id):
                        from_account_count += 1
                    
                    if bankaccount_id_value and str(bankaccount_id_value) == str(to_bank_account_id):
                        to_account_count += 1
                
                from_account_matched = from_account_count > 0
                to_account_matched = to_account_count > 0
                
                print(f"  [Debug] PayPal account ({from_bank_account_id}) match count: {from_account_count}")
                print(f"  [Debug] Bank account ({to_bank_account_id}) match count: {to_account_count}")

                # --- Step 1: Check for three-way matching FIRST (PayPal with fees) ---
                # Only check if neither side is already matched
                three_way_match = None
                if from_account_count == 0 and to_account_count == 0:
                    three_way_match = self._check_three_way_match(pt, available_bts)
                    if three_way_match:
                        three_way_matches.append(three_way_match)
                        # Remove the matched bank transaction from available list
                        if three_way_match.paypal_bank_transaction in available_bts:
                            available_bts.remove(three_way_match.paypal_bank_transaction)
                        print(f"  [Debug] Three-way match found, skipping simple matching")
                        unmatched_pts.remove(pt) # Remove from unmatched list
                        continue  # Skip simple matching if three-way match found

                # --- Step 2: Find PayPal side match (positive amount on FROM account) ---
                if from_account_count == 0:
                    for bt in available_bts:
                        bank_txn_date = datetime.strptime(bt['date'], '%Y-%m-%d')
                        bank_txn_name = bt.get('name', '').lower()
                        
                        if (bt.get('amount') == transfer_amount and
                            bt.get('bankaccount_id') == from_bank_account_id and
                            abs((bank_txn_date - transfer_date).days) <= 7 and
                            any(re.search(k, bank_txn_name) for k in BETTING_PAYPAL_KEYWORDS)):
                            paypal_side_match = bt
                            print(f"  [Debug] Found Simple PayPal side match: BT ID {bt.get('id')}")
                            break

                # --- Step 3: Find Bank side match (negative amount on TO account) ---
                if to_account_count == 0:
                    for bt in available_bts:
                        if bt == paypal_side_match: continue  # Don't match same transaction
                        if not (bt.get('amount', 0) < 0): continue  # Must be negative
                        
                        bank_txn_date = datetime.strptime(bt['date'], '%Y-%m-%d')
                        bank_txn_name = bt.get('name', '').lower()
                        
                        if (bt.get('amount') == -transfer_amount and
                            abs((bank_txn_date - transfer_date).days) <= 7 and
                            bt.get('bankaccount_id') == to_bank_account_id and
                            any(re.search(k, bank_txn_name) for k in BETTING_BANK_TRANSFER_KEYWORDS)):
                            bank_side_match = bt
                            print(f"  [Debug] Found Bank side candidate: BT ID {bt.get('id')}")
                            break

            except (ValueError, KeyError) as e:
                print(f"Data error during match search for PT ID {pt.get('id')}: {e}")
                continue

            # --- Step 4: Validate and Create Simple Matches ---
            is_matched = False

            # Case 1: Two-legged simple match (Both PayPal and Bank sides are found)
            if paypal_side_match and bank_side_match:
                # CRITICAL: Prevent matching two bank transactions from the same bank account ID
                if paypal_side_match.get('bankaccount_id') == bank_side_match.get('bankaccount_id'):
                    print(f"  [Debug] Simple Match Validation FAIL: Both candidates from same bankaccount_id ({paypal_side_match.get('bankaccount_id')}). Discarding match.")
                else:
                    print(f"  [Debug] Found two-legged simple match. Creating matches.")
                    # Match the PayPal side
                    simple_matches.append(SimpleMatch(
                        platform_transaction=pt,
                        bank_transaction=paypal_side_match,
                        match_date=datetime.now().isoformat()
                    ))
                    if paypal_side_match in available_bts:
                        available_bts.remove(paypal_side_match)
                    
                    # Match the Bank side
                    simple_matches.append(SimpleMatch(
                        platform_transaction=pt,
                        bank_transaction=bank_side_match,
                        match_date=datetime.now().isoformat()
                    ))
                    if bank_side_match in available_bts:
                        available_bts.remove(bank_side_match)
                    
                    is_matched = True

            # Case 2: One-legged simple match (Only the Betting Bank side is found)
            elif bank_side_match and not paypal_side_match:
                print(f"  [Debug] Found one-legged simple match (Bank side only). Creating match.")
                simple_matches.append(SimpleMatch(
                    platform_transaction=pt,
                    bank_transaction=bank_side_match,
                    match_date=datetime.now().isoformat()
                ))
                if bank_side_match in available_bts:
                    available_bts.remove(bank_side_match)
                
                is_matched = True
            
            # Case 3 (Implicit): Only PayPal side is found. This is NOT a match and will fall to unmatched analysis.

            if is_matched:
                if pt in unmatched_pts:
                    unmatched_pts.remove(pt)
            
            # Only include in unmatched analysis if the transaction has not been matched
            # and has no existing related bank transactions.
            if not is_matched and from_account_count == 0 and to_account_count == 0:
                # Post-mortem analysis for why the transaction was not matched
                reasons = []
                try:
                    transfer_amount = pt.get('Amount')
                    transfer_date = datetime.strptime(pt['Date'], '%Y-%m-%d')
                    from_bank_account_id = pt.get('From_Account')
                    to_bank_account_id = pt.get('To_Account')

                    # --- Analyze PayPal side (FROM leg) ---
                    from_candidates = [bt for bt in self.bank_transactions if bt.get('bankaccount_id') == from_bank_account_id]
                    if not from_candidates:
                        reasons.append(f"PayPal side: No BTs found for account ID {from_bank_account_id}.")
                    else:
                        amount_matches = [bt for bt in from_candidates if bt.get('amount') == transfer_amount]
                        if not amount_matches:
                            reasons.append(f"PayPal side: No BT with amount {transfer_amount} found for account ID {from_bank_account_id}.")
                        else:
                            date_matches = [bt for bt in amount_matches if abs((datetime.strptime(bt['date'], '%Y-%m-%d') - transfer_date).days) <= 7]
                            if not date_matches:
                                dates = sorted([bt['date'] for bt in amount_matches])
                                reasons.append(f"PayPal side: Found BT with correct amount, but date was outside 7-day window. Candidate dates: {dates}")
                            else:
                                keyword_matches = [bt for bt in date_matches if any(re.search(k, bt.get('name', '').lower()) for k in BETTING_PAYPAL_KEYWORDS)]
                                if not keyword_matches:
                                    names = [bt['name'] for bt in date_matches]
                                    reasons.append(f"PayPal side: Found BT with correct amount/date, but description did not match keywords. Candidate names: {names}")
                    
                    # --- Analyze Bank side (TO leg) ---
                    to_candidates = [bt for bt in self.bank_transactions if bt.get('bankaccount_id') == to_bank_account_id]
                    if not to_candidates:
                        reasons.append(f"Bank side: No BTs found for account ID {to_bank_account_id}.")
                    else:
                        amount_matches = [bt for bt in to_candidates if bt.get('amount') == -transfer_amount]
                        if not amount_matches:
                            reasons.append(f"Bank side: No BT with amount {-transfer_amount} found for account ID {to_bank_account_id}.")
                        else:
                            date_matches = [bt for bt in amount_matches if abs((datetime.strptime(bt['date'], '%Y-%m-%d') - transfer_date).days) <= 7]
                            if not date_matches:
                                dates = sorted([bt['date'] for bt in amount_matches])
                                reasons.append(f"Bank side: Found BT with correct amount, but date was outside 7-day window. Candidate dates: {dates}")
                            else:
                                keyword_matches = [bt for bt in date_matches if any(re.search(k, bt.get('name', '').lower()) for k in BETTING_BANK_TRANSFER_KEYWORDS)]
                                if not keyword_matches:
                                    names = [bt['name'] for bt in date_matches]
                                    reasons.append(f"Bank side: Found BT with correct amount/date, but description did not match keywords. Candidate names: {names}")

                except (ValueError, KeyError) as e:
                    reasons.append(f"An error occurred during analysis: {e}")

                unmatched_info.append(UnmatchedTransactionInfo(
                    platform_transaction=pt,
                    reason="\n".join(reasons) if reasons else "No potential bank transaction candidates found."
                ))

        return MatchResults(
            simple_matches=simple_matches,
            three_way_matches=three_way_matches,
            unmatched_platform_transactions=unmatched_info
        )

    def _check_three_way_match(self, pt: Dict[str, Any], available_bts: List[Dict[str, Any]]) -> ThreeWayMatch:
        """
        Checks if a platform transaction can be matched with both PayPal and Bank transactions that include fees.
        This handles the case where a PayPal transfer has fees, requiring splitting into multiple platform transactions.
        
        Logic:
        1. PayPal side: bt_amount == transfer_amount (matches Gross from scraped transaction)
        2. Bank side: bt_amount == -(transfer_amount - fees) (matches Net from scraped transaction, negative)
        3. Scraped verification: Gross = transfer_amount, Net = transfer_amount - fees
        """
        try:
            transfer_amount = pt.get('Amount')
            transfer_date = datetime.strptime(pt['Date'], '%Y-%m-%d')
            from_account = pt.get('from', {})
            to_account = pt.get('to', {})
            from_bank_account_id = from_account.get('bankaccount_id')  # PayPal account
            to_bank_account_id = to_account.get('bankaccount_id')  # Bank account
            
            print(f"  [Debug] Checking three-way match for PT {pt.get('id')} (amount: {transfer_amount})")
            
            # Step 1: Find a candidate PayPal BT where amount >= PT amount and verify with scraped data
            paypal_bt = None
            scraped_match = None

            for bt_candidate in available_bts:
                if (bt_candidate.get('amount', 0) >= transfer_amount and
                    bt_candidate.get('bankaccount_id') == from_bank_account_id):
                    
                    bt_date_str = bt_candidate.get('date')
                    bt_date = datetime.strptime(bt_date_str, '%Y-%m-%d')

                    if abs((bt_date - transfer_date).days) <= 7:
                        # Found a potential BT. Now, verify with scraped data.
                        potential_scraped_match = self._find_matching_scraped_transaction(
                            bt_candidate.get('amount'), 
                            bt_date_str
                        )

                        if potential_scraped_match:
                            scraped_gross = round(abs(float(potential_scraped_match.get('Gross', 0))), 2)
                            scraped_net = round(abs(float(potential_scraped_match.get('Net', 0))), 2)

                            # Final verification: PT amount must match either Gross or Net
                            if transfer_amount == scraped_gross or transfer_amount == scraped_net:
                                # CRITICAL: Only proceed if there is a fee
                                if scraped_gross != scraped_net:
                                    paypal_bt = bt_candidate
                                    scraped_match = potential_scraped_match
                                    print(f"  [Debug] Found PayPal side match: BT {paypal_bt.get('id')} (${paypal_bt.get('amount')})")
                                    print(f"  [Debug] Verified with scraped match: ID {scraped_match.get('id')}, Gross=${scraped_gross}, Net=${scraped_net}")
                                    break # Exit loop once a fully verified match is found
            
            # Fallback: Check if we can match using only scraped data and the bank-side transaction
            # This handles cases where the PayPal-side bank transaction might be missing.
            if not paypal_bt:
                print("  [Debug] PayPal BT not found, attempting fallback using scraped data...")
                for st in self.scraped_transactions:
                    if st.get('Type') == 'transfer_sent':
                        st_gross = round(abs(float(st.get('Gross', 0))), 2)
                        st_net = round(abs(float(st.get('Net', 0))), 2)

                        # Check if PT amount matches either Gross or Net from scraped data
                        if transfer_amount == st_gross or transfer_amount == st_net:
                            st_date = datetime.strptime(st.get('Transaction Date'), '%Y-%m-%d')
                            if abs((st_date - transfer_date).days) <= 7:
                                # Found a potential scraped match. Now, find the bank-side BT.
                                expected_bank_amount = -st_net
                                bank_bt_fallback = find_corresponding_bank_transaction(
                                    to_bank_account_id,
                                    expected_bank_amount,
                                    st.get('Transaction Date'),
                                    available_bts
                                )

                                if bank_bt_fallback:
                                    # CRITICAL: Only proceed if there is a fee
                                    if st_gross != st_net:
                                        scraped_fees = st_gross - st_net
                                        print(f"  [Debug] Fallback match found: Scraped ID {st.get('id')} + Bank BT {bank_bt_fallback.get('id')}")
                                        return ThreeWayMatch(
                                            original_platform_transaction=pt,
                                            paypal_bank_transaction=None, # Explicitly set to None
                                            bank_side_transaction=bank_bt_fallback,
                                            scraped_transaction=st,
                                            net_amount=st_net,
                                            fee_amount=round(scraped_fees, 2),
                                            match_date=datetime.now().isoformat()
                                        )

            # If we've reached here with a valid paypal_bt, proceed with the original three-way logic
            if not paypal_bt or not scraped_match:
                return None

            # Step 2: Calculate fees and find the corresponding bank-side transaction
            scraped_gross = round(abs(float(scraped_match.get('Gross', 0))), 2)
            scraped_net = round(abs(float(scraped_match.get('Net', 0))), 2)
            scraped_fees = scraped_gross - scraped_net
            
            print(f"  [Debug] Calculated fees: ${scraped_fees} (Gross=${scraped_gross} - Net=${scraped_net})")
            
            # Look for bank-side transaction with the Net amount (negative)
            expected_bank_amount = -scraped_net
            bank_bt = None
            
            for bt in available_bts:
                if (bt != paypal_bt and  # Don't match same transaction
                    bt.get('amount') == expected_bank_amount and
                    bt.get('bankaccount_id') == to_bank_account_id):
                    
                    bank_txn_date = datetime.strptime(bt['date'], '%Y-%m-%d')
                    if abs((bank_txn_date - transfer_date).days) <= 7:
                        bank_bt = bt
                        print(f"  [Debug] Found bank side match: BT {bt.get('id')} (${bt.get('amount')})")
                        break
            
            if bank_bt:
                print(f"  [Debug] Found three-way match: PayPal BT {paypal_bt.get('id')} (${paypal_bt.get('amount')}) + Bank BT {bank_bt.get('id')} (${bank_bt.get('amount')}) with fees ${round(scraped_fees, 2)}")
                return ThreeWayMatch(
                    original_platform_transaction=pt,
                    paypal_bank_transaction=paypal_bt,
                    bank_side_transaction=bank_bt,
                    scraped_transaction=scraped_match,
                    net_amount=scraped_net,
                    fee_amount=round(scraped_fees, 2),
                    match_date=datetime.now().isoformat()
                )
            else:
                print(f"  [Debug] No bank side match found for amount ${expected_bank_amount}")
                        
        except (ValueError, KeyError) as e:
            print(f"  [Debug] Error in three-way match check: {e}")
            
        return None

    def _find_matching_scraped_transaction(self, gross_amount: float, date: str) -> Dict[str, Any]:
        """Finds a scraped transaction that matches the gross amount and date."""
        try:
            target_date = datetime.strptime(date, '%Y-%m-%d')
            
            for st in self.scraped_transactions:
                # Scraped transactions have negative values for outgoing transfers
                # Convert to positive for comparison and round to 2 decimal places
                st_gross = round(abs(float(st.get('Gross', 0))), 2) if st.get('Gross') else 0
                
                if (st_gross == gross_amount and 
                    st.get('Type') == 'transfer_sent'):
                    
                    # Check date within 3 days - use 'Transaction Date' field
                    st_date_str = st.get('Transaction Date', '')
                    if st_date_str:
                        st_date = datetime.strptime(st_date_str, '%Y-%m-%d')
                        if abs((st_date - target_date).days) <= 3:
                            print(f"  [Debug] Found matching scraped transaction: ID {st.get('id')}, Gross ${st_gross}")
                            return st
                        
        except (ValueError, KeyError) as e:
            print(f"  [Debug] Error finding scraped transaction: {e}")
            
        return None

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

def get_scraped_transactions(player_id: int) -> List[Dict[str, Any]]:
    """Fetches all scraped transactions for a given player."""
    try:
        params = {'user_id': player_id}
        response = requests.get(SCRAPED_TRANSACTION_API_URL, params=params)
        response.raise_for_status()
        return response.json()
    except (requests.RequestException, ValueError) as e:
        print(f"Error fetching scraped transactions: {e}")
        return []

def execute_three_way_match(three_way_match: ThreeWayMatch, player_id: int, all_bank_transactions: List[Dict[str, Any]]):
    """
    Executes the three-way matching by creating new platform transactions and linking them.
    """
    try:
        pt = three_way_match.original_platform_transaction
        paypal_bt = three_way_match.paypal_bank_transaction
        
        from_account_details = pt.get('from', {})
        to_account_details = pt.get('to', {})
        original_betting_bank_account_id = pt.get('To_Account') # Use top-level ID

        print("\n[DEBUG] Account Information from Original PT:")
        print(f"  FROM Account (Top-Level): {pt.get('From_Account')}")
        print(f"  TO Account (Top-Level):   {pt.get('To_Account')}")
        # --- End Debugging ---
        
        print(f"📋 Three-Way Match Details:")
        print(f"  Original PT: {pt.get('id')} (${pt.get('Amount')}) - From ID {pt.get('From_Account')} -> To ID {pt.get('To_Account')}")
        if paypal_bt:
            print(f"  PayPal BT: {paypal_bt.get('id')} (${paypal_bt.get('amount')}) - {paypal_bt.get('name')}")
        scraped_txn = three_way_match.scraped_transaction
        print(f"  Scraped: Gross=${abs(float(scraped_txn.get('Gross', 0)))}, Fee=${abs(float(scraped_txn.get('Fee', 0)))}, Net=${abs(float(scraped_txn.get('Net', 0)))}")
        
        paypal_bank_account_id = from_account_details.get('bankaccount_id')

        print(f"[DEBUG] Derived ID for new Transfer PT 'To_Account': {original_betting_bank_account_id}")
        # --- End Debugging ---
        
        print(f"📋 Three-Way Match Details:")
        print(f"  FROM: Transfer Account -> {to_account_details.get('Account_Name')} (ID: {original_betting_bank_account_id})")
        print(f"  AMOUNT: ${three_way_match.net_amount}")
        print(f"  TYPE: transfer")
        
        # --- Payloads for API calls ---
        update_payload = {
            'Amount': abs(paypal_bt.get('amount')) if paypal_bt else abs(three_way_match.net_amount + three_way_match.fee_amount),
            'Date': paypal_bt.get('date') if paypal_bt else three_way_match.scraped_transaction.get('Transaction Date'),
            'User_ID': player_id,
            'To_Account': TRANSFER_ACCOUNT_ID
        }
        
        fees_changes = {
            'Transaction_Type': 'fees',
            'Amount': abs(three_way_match.fee_amount),
            'Date': paypal_bt.get('date') if paypal_bt else three_way_match.scraped_transaction.get('Transaction Date'),
            'From_Account': TRANSFER_ACCOUNT_ID,
            'To_Account': FEES_ACCOUNT_ID,
            'User_ID': player_id,
            'Added_By': AI_USER_ID,
            'created_at': datetime.now().isoformat()
        }
        
        transfer_changes = {
            'Transaction_Type': 'transfer',
            'Amount': abs(three_way_match.net_amount),
            'Date': paypal_bt.get('date') if paypal_bt else three_way_match.scraped_transaction.get('Transaction Date'),
            'From_Account': TRANSFER_ACCOUNT_ID,
            'To_Account': original_betting_bank_account_id,
            'User_ID': player_id,
            'Added_By': AI_USER_ID,
            'created_at': datetime.now().isoformat()
        }

        # --- Final Pre-API Call Verification ---
        print("\n[VERIFICATION] Final check before creating Transfer PT:")
        print(f"  Payload 'To_Account' ID: {transfer_changes['To_Account']}")
        print(f"  Target Account Name (from original PT): {to_account_details.get('Account_Name')}")
        # --- End Verification ---

        # --- Execute API Calls ---
        print(f"\n--- Executing Three-Way Match for PT {pt.get('id')} ---")
        
        updated_pt = update_platform_transaction(pt.get('id'), pt, update_payload)
        if not updated_pt: return
        
        fees_pt = create_platform_transaction(fees_changes)
        transfer_pt = create_platform_transaction(transfer_changes)
        if not fees_pt or not transfer_pt: return

        # --- Link Bank Transactions ---
        if paypal_bt:
            link_bank_transaction(paypal_bt.get('transaction_id'), updated_pt.get('id'))
        
        bank_bt_to_link = three_way_match.bank_side_transaction
        if bank_bt_to_link:
            link_bank_transaction(bank_bt_to_link.get('transaction_id'), transfer_pt.get('id'))

    except Exception as e:
        print(f"  ❌ An unexpected error occurred during execution: {e}")

def find_corresponding_bank_transaction(bank_account_id: int, amount: float, date: str, all_bank_transactions: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Finds a bank transaction that corresponds to the given parameters."""
    print(f"Looking for bank transaction: account_id={bank_account_id}, amount={amount}, date={date}")
    target_date = datetime.strptime(date, '%Y-%m-%d')

    for bt in all_bank_transactions:
        if (bt.get('bankaccount_id') == bank_account_id and
                bt.get('amount') == amount):
            bt_date = datetime.strptime(bt.get('date'), '%Y-%m-%d')
            # Allow a small window for date matching as well, just in case
            if abs((bt_date - target_date).days) <= 7:
                return bt
    return None

# --- API Interaction Functions ---

def link_bank_transaction(bank_transaction_id: str, platform_transaction_id: int):
    """Links a bank transaction to a platform transaction."""
    url = UPDATE_BANK_TRANSACTIONS_API_URL.format(transaction_id=bank_transaction_id)
    payload = {"transaction_link": platform_transaction_id, "last_edited_by": AI_USER_ID}
    try:
        response = requests.patch(url, json=payload)
        response.raise_for_status()
        print(f"  ✅ Successfully linked BT {bank_transaction_id} to PT {platform_transaction_id}")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"  ❌ Failed to link BT {bank_transaction_id}: {e}")
        return None

def update_platform_transaction(pt_id: int, original_pt: dict, changes: dict):
    """Updates an existing platform transaction."""
    url = UPDATE_PLATFORM_TRANSACTIONS_API_URL.format(platform_transaction_id=pt_id)
    try:
        response = requests.patch(url, json=changes)
        response.raise_for_status()
        print(f"  ✅ Successfully updated PT {pt_id}")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"  ❌ Failed to update PT {pt_id}: {e}")
        return None

def create_platform_transaction(payload: dict):
    """Creates a new platform transaction from a complete payload."""
    try:
        response = requests.post(CREATE_PLATFORM_TRANSACTION_API_URL, json=payload)
        response.raise_for_status()
        print(f"  ✅ Successfully created new PT.")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"  ❌ Failed to create new PT: {e}")
        return None
