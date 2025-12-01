#!/usr/bin/env python3
"""
Simplified PayPal to Bank Transfer Matcher
Matches platform transfers from Betting PayPal to Betting Bank, with a clear,
prioritized logic for handling three-way matches with fees and simple one-to-one matches.
"""

import requests
import os
import re
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from config import (
    PLATFORM_TRANSACTIONS_API_URL, BANK_TRANSACTIONS_API_URL, 
    SCRAPED_TRANSACTION_API_URL
)

# --- Keywords (Data-Driven from CSV Analysis) ---
# Keywords to identify the outgoing transaction from the PayPal account
BETTING_PAYPAL_KEYWORDS = [
    "money transfer to",
]

# Keywords to identify the incoming transaction to the Betting Bank account
BETTING_BANK_TRANSFER_KEYWORDS = [
    "^paypal transfer$",
    "paypal transfer",
    "transfer.*paypal",
    "^paypal",
    "rtp.*paypal",
    "instant.*paypal",
    "money transfer authorized"
]

# --- Data Classes for Structure ---
@dataclass
class SimpleMatch:
    """Represents a simple one-to-one or one-to-two match."""
    platform_transaction: Dict[str, Any]
    bank_transactions: List[Dict[str, Any]] # Can be one or two bank transactions
    match_date: str

@dataclass
class ThreeWayMatch:
    """Represents a complex match involving a fee, requiring a transaction split."""
    original_platform_transaction: Dict[str, Any]
    scraped_transaction: Dict[str, Any]
    paypal_bank_transaction: Dict[str, Any]  # The bank transaction matching the gross amount
    bank_side_transaction: Optional[Dict[str, Any]]    # The bank transaction matching the net amount
    
    # Details required for execution
    gross_amount: float
    net_amount: float
    fee_amount: float
    match_date: str

@dataclass
class UnmatchedTransactionInfo:
    """Represents an unmatched transaction with a reason for the failure."""
    platform_transaction: Dict[str, Any]
    reason: str

@dataclass
class MatchResults:
    """Container for all matching results from a single run."""
    simple_matches: List[SimpleMatch]
    three_way_matches: List[ThreeWayMatch]
    unmatched_platform_transactions: List[UnmatchedTransactionInfo]

# --- Main Matcher Class ---
class SimplifiedPayPalMatcher:
    def __init__(self, platform_transactions: List[Dict[str, Any]], bank_transactions: List[Dict[str, Any]], scraped_transactions: List[Dict[str, Any]]):
        self.platform_transactions = platform_transactions
        self.bank_transactions = bank_transactions
        self.scraped_transactions = scraped_transactions
        self._preprocess_data()

    def _preprocess_data(self):
        """
        Step 1: Cleans and prepares the data by filtering for relevant transactions.
        """
        # Filter platform_transactions for relevant transfers:
        # - From 'Betting PayPal' to 'Betting Bank'
        # - Not already fully matched (i.e., less than 2 linked bank transactions)
        self.platform_transactions = [
            pt for pt in self.platform_transactions
            if (pt.get('Transaction_Type', '').lower() == 'transfer' and
                len(pt.get('related_bank_transaction', [])) < 2 and
                pt.get('from', {}).get('Account_Type', '').lower() == 'betting paypal account' and
                pt.get('to', {}).get('Account_Type', '').lower() == 'betting bank account')
        ]
        
        # Filter bank_transactions to only include those that are not yet linked.
        self.bank_transactions = [
            bt for bt in self.bank_transactions if not bt.get('linked_transaction')
        ]

        # Build Bank Transaction Index: (account_id, amount) -> List[bt]
        self.bank_index = {}
        for bt in self.bank_transactions:
            # Pre-parse date for faster comparison
            if isinstance(bt.get('date'), str):
                try:
                    bt['parsed_date'] = datetime.strptime(bt.get('date'), '%Y-%m-%d')
                except ValueError:
                    continue # Skip invalid dates
            
            key = (bt.get('bankaccount_id'), round(bt.get('amount', 0), 2))
            if key not in self.bank_index:
                self.bank_index[key] = []
            self.bank_index[key].append(bt)

        # Build Scraped Transaction Indexes
        self.scraped_gross_index = {}
        self.scraped_net_index = {}
        for st in self.scraped_transactions:
            if isinstance(st.get('Transaction Date'), str):
                try:
                    st['parsed_date'] = datetime.strptime(st.get('Transaction Date'), '%Y-%m-%d')
                except ValueError:
                    continue

            gross = st.get('Gross')
            if gross is not None:
                try:
                    g_val = round(abs(float(gross)), 2)
                    if g_val not in self.scraped_gross_index:
                        self.scraped_gross_index[g_val] = []
                    self.scraped_gross_index[g_val].append(st)
                except ValueError:
                    pass
            
            net = st.get('Net')
            if net is not None:
                try:
                    n_val = round(abs(float(net)), 2)
                    if n_val not in self.scraped_net_index:
                        self.scraped_net_index[n_val] = []
                    self.scraped_net_index[n_val].append(st)
                except ValueError:
                    pass

    def match_transactions(self) -> MatchResults:
        """
        Step 2: Iterates through platform transactions and attempts to find a match.
        It prioritizes the complex Three-Way Match first before attempting a Simple Match.
        """
        simple_matches = []
        three_way_matches = []
        unmatched_info = []
        
        # Track used bank transaction IDs to prevent double usage
        used_bt_ids = set()
        
        # Create a set of PT IDs that have been matched to avoid processing them twice
        matched_pt_ids = set()

        for pt in self.platform_transactions:
            if pt.get('id') in matched_pt_ids:
                continue

            # Priority 1: Attempt to find a three-way match involving fees.
            three_way_match = self._find_three_way_match(pt, used_bt_ids)
            if three_way_match:
                three_way_matches.append(three_way_match)
                matched_pt_ids.add(pt.get('id'))
                # Mark bank transactions as used
                if three_way_match.paypal_bank_transaction:
                    used_bt_ids.add(three_way_match.paypal_bank_transaction.get('id'))
                if three_way_match.bank_side_transaction:
                    used_bt_ids.add(three_way_match.bank_side_transaction.get('id'))
                continue

            # Priority 2: If no three-way match, attempt a simple match.
            simple_match = self._find_simple_match(pt, used_bt_ids)
            if simple_match:
                simple_matches.append(simple_match)
                matched_pt_ids.add(pt.get('id'))
                # Mark bank transactions as used
                for bt in simple_match.bank_transactions:
                    used_bt_ids.add(bt.get('id'))
                continue
        
        # Add any remaining, unprocessed platform transactions to the unmatched list
        for pt in self.platform_transactions:
            if pt.get('id') not in matched_pt_ids:
                unmatched_info.append(UnmatchedTransactionInfo(
                    platform_transaction=pt,
                    reason="No simple or three-way match found based on the defined criteria."
                ))

        return MatchResults(simple_matches, three_way_matches, unmatched_info)

    def _find_three_way_match(self, pt: Dict[str, Any], used_bt_ids: set) -> ThreeWayMatch:
        """
        Attempts to find a three-way match for a given platform transaction.
        """
        # Per user request, only attempt a three-way match if the PT is completely un-linked.
        if len(pt.get('related_bank_transaction', [])) != 0:
            return None
            
        pt_amount = round(pt.get('Amount', 0), 2)
        pt_date = datetime.strptime(pt['Date'], '%Y-%m-%d')
        from_bank_account_id = pt.get('from', {}).get('bankaccount_id')
        to_bank_account_id = pt.get('to', {}).get('bankaccount_id')

        # 1. Verify with Scraped Data
        # Find a scraped transaction that matches the gross amount or net amount
        candidates = []
        if pt_amount in self.scraped_gross_index:
            candidates.extend(self.scraped_gross_index[pt_amount])
        if pt_amount in self.scraped_net_index:
            for st in self.scraped_net_index[pt_amount]:
                # Simple deduplication if object identity is preserved
                if st not in candidates:
                    candidates.append(st)

        for st in candidates:
            # Gracefully handle cases where 'Gross' might be None
            gross_amount_str = st.get('Gross')
            if gross_amount_str is None:
                continue

            st_gross = round(abs(float(gross_amount_str)), 2)
            st_net = round(abs(float(st.get('Net', 0))), 2)
            st_date = st.get('parsed_date') # Use pre-parsed date
            if not st_date:
                 continue

            # Check if dates are within a reasonable window (e.g., 7 days)
            if abs((st_date - pt_date).days) <= 7:
                
                # 2. Check for Fees
                fee_amount = round(st_gross - st_net, 2)
                if fee_amount > 0.01:
                    
                    # 3. Find the PayPal Transaction (matching the gross amount)
                    paypal_bt = self._find_bank_transaction(
                        account_id=from_bank_account_id,
                        amount=st_gross,
                        date=st_date,
                        used_bt_ids=used_bt_ids
                    )
                    
                    # 4. Find the Bank-Side Transaction (matching the net amount)
                    bank_side_bt = self._find_bank_transaction(
                        account_id=to_bank_account_id,
                        amount=-st_net, # Bank deposits are negative
                        date=st_date,
                        used_bt_ids=used_bt_ids
                    )

                    # If the gross leg (PayPal side) is found, and a fee exists, we have enough confidence to proceed.
                    if paypal_bt:
                        return ThreeWayMatch(
                            original_platform_transaction=pt,
                            scraped_transaction=st,
                            paypal_bank_transaction=paypal_bt,
                            bank_side_transaction=bank_side_bt, # This can be None
                            gross_amount=st_gross,
                            net_amount=st_net,
                            fee_amount=fee_amount,
                            match_date=datetime.now().isoformat()
                        )
        return None

    def _find_simple_match(self, pt: Dict[str, Any], used_bt_ids: set) -> SimpleMatch:
        """
        Attempts to find a simple one-to-one or one-to-two match for a platform transaction.
        """
        pt_amount = round(pt.get('Amount', 0), 2)
        pt_date = datetime.strptime(pt['Date'], '%Y-%m-%d')
        from_bank_account_id = pt.get('from', {}).get('bankaccount_id')
        to_bank_account_id = pt.get('to', {}).get('bankaccount_id')
        matched_bts = []

        # 1. The PayPal Side (positive amount from the 'from' account)
        paypal_side_bt = self._find_bank_transaction(
            account_id=from_bank_account_id,
            amount=pt_amount,
            date=pt_date,
            used_bt_ids=used_bt_ids,
            keywords=BETTING_PAYPAL_KEYWORDS
        )
        if paypal_side_bt:
            matched_bts.append(paypal_side_bt)

        # 2. The Bank Side (negative amount to the 'to' account)
        bank_side_bt = self._find_bank_transaction(
            account_id=to_bank_account_id,
            amount=-pt_amount,
            date=pt_date,
            used_bt_ids=used_bt_ids,
            keywords=BETTING_BANK_TRANSFER_KEYWORDS
        )
        if bank_side_bt:
            matched_bts.append(bank_side_bt)

        # A match is valid if we found at least one corresponding bank transaction.
        # Per the logic, we prioritize matches where the bank deposit is found.
        if bank_side_bt:
            return SimpleMatch(
                platform_transaction=pt,
                bank_transactions=matched_bts,
                match_date=datetime.now().isoformat()
            )
        return None

    def _find_bank_transaction(self, account_id: int, amount: float, date: datetime, used_bt_ids: set, keywords: List[str] = None) -> Dict[str, Any]:
        """A helper to find a single bank transaction that meets the criteria."""
        target_amount = round(amount, 2)
        
        # Use index for O(1) lookup of candidates
        key = (account_id, target_amount)
        if key not in self.bank_index:
            return None
            
        candidates = self.bank_index[key]
        
        for bt in candidates:
            if bt.get('id') in used_bt_ids:
                continue

            bt_date = bt.get('parsed_date')
            if not bt_date:
                 continue

            if abs((bt_date - date).days) <= 7:
                if keywords:
                    description = bt.get('name', '').lower()
                    if any(re.search(k, description) for k in keywords):
                        return bt
                else:
                    # If no keywords are provided, amount, date, and account are enough
                    return bt
        return None

# --- Data Fetching Functions (to be used by the runner script) ---

def get_all_platform_transactions(player_id: int) -> List[Dict[str, Any]]:
    """Fetches all platform transactions for a given player."""
    try:
        params = {'user_id': player_id}
        response = requests.get(PLATFORM_TRANSACTIONS_API_URL, params=params)
        response.raise_for_status()
        return response.json()
    except (requests.RequestException, ValueError) as e:
        print(f"Error fetching platform transactions: {e}")
        return []

def get_all_bank_transactions(player_id: int) -> List[Dict[str, Any]]:
    """Fetches all bank transactions for a given player."""
    try:
        params = {'player_id': player_id}
        response = requests.get(BANK_TRANSACTIONS_API_URL, params=params)
        response.raise_for_status()
        return response.json().get('bankTransactions', [])
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
