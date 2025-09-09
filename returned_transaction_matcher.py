#!/usr/bin/env python3
"""
Simple transaction matcher for the Received/Returned Transaction Matching Algorithm
Contains the core logic for matching returned platform transactions with bank transactions.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Any
from dataclasses import dataclass

# Returned transaction keywords
RETURNED_KEYWORDS = ["checkbook", "reel ventures", "rv enhanced wall", "individual"]


@dataclass
class MatchCriteria:
    """Data class for match criteria validation results"""
    amount_match: bool
    date_match: bool
    keyword_match: bool
    user_match: bool
    is_direct_match: bool


@dataclass
class Match:
    """Data class for a matched transaction pair"""
    platform_transaction: Dict[str, Any]
    bank_transaction: Dict[str, Any]
    match_date: str
    match_criteria: MatchCriteria


@dataclass
class Summary:
    """Data class for matching summary statistics"""
    total_matches: int
    total_returned_platform_transactions: int
    total_potential_bank_transactions: int
    match_rate: float


@dataclass
class MatchResults:
    """Data class for complete matching results"""
    matches: List[Match]
    unmatched_platform_transactions: List[Dict[str, Any]]
    unmatched_bank_transactions: List[Dict[str, Any]]
    summary: Summary


class SimpleTransactionMatcher:
    """Simple transaction matcher focusing on core logic"""
    
    def __init__(self, platform_transactions: List[Dict[str, Any]], 
                 bank_transactions: List[Dict[str, Any]], 
                 returned_keywords: List[str]):
        """
        Initialize the transaction matcher
        
        Args:
            platform_transactions: List of platform transaction dictionaries
            bank_transactions: List of bank transaction dictionaries
            returned_keywords: List of keywords to identify returned transactions
        """
        self.platform_transactions = platform_transactions
        self.bank_transactions = bank_transactions
        self.returned_keywords = returned_keywords
        
        # Pre-process data for matching
        self._preprocess_data()
    
    def _preprocess_data(self):
        """Pre-process data for matching"""
        # Filter returned platform transactions
        self._returned_platform_transactions = [
            pt for pt in self.platform_transactions 
            if pt.get('Transaction_Type') == 'returned'
        ]
        
        # Filter potential bank transactions (positive amounts and containing keywords)
        self._potential_bank_transactions = []
        for bt in self.bank_transactions:
            if bt.get('amount', 0) > 0:  # Only positive amounts
                # Check if transaction name contains any of the keywords
                transaction_name = bt.get('name', '').lower()
                if any(keyword.lower() in transaction_name for keyword in self.returned_keywords):
                    self._potential_bank_transactions.append(bt)
    
    def _is_amount_match(self, platform_amount: float, bank_amount: float) -> bool:
        """Check if amounts match within $0.01 tolerance"""
        return abs(platform_amount - bank_amount) <= 0.01
    
    def _is_date_match(self, platform_date: datetime, bank_date: datetime) -> bool:
        """Check if dates are within 5 days of each other"""
        date_diff = abs(platform_date - bank_date)
        return date_diff <= timedelta(days=5)
    
    def _is_bank_account_match(self, platform_transaction: Dict[str, Any], 
                              bank_transaction: Dict[str, Any]) -> bool:
        """Check if bank account IDs match"""
        platform_from_account = platform_transaction.get('from', {})
        platform_bank_account_id = platform_from_account.get('bankaccount_id')
        bank_account_id = bank_transaction.get('bankaccount_id')
        
        return platform_bank_account_id == bank_account_id
    
    def match_returned_transactions(self) -> MatchResults:
        """
        Match returned platform transactions with bank transactions
        
        Returns:
            MatchResults object containing all matches and statistics
        """
        matches = []
        matched_bank_transaction_ids = set()
        matched_platform_transaction_ids = set()

        # Create a quick lookup for bank transactions by ID
        bank_transactions_by_id = {bt['id']: bt for bt in self.bank_transactions}

        # --- Step 1: Process direct matches from 'related_bank_transaction' ---
        for pt in self._returned_platform_transactions:
            related_bt_info = pt.get('related_bank_transaction')
            if related_bt_info and isinstance(related_bt_info, list) and len(related_bt_info) > 0:
                related_bt_id = related_bt_info[0].get('id')
                
                bank_transaction = bank_transactions_by_id.get(related_bt_id)
                
                if bank_transaction:
                    match_criteria = MatchCriteria(
                        amount_match=True, date_match=True, keyword_match=False,
                        user_match=True, is_direct_match=True
                    )
                    
                    match = Match(
                        platform_transaction=pt,
                        bank_transaction=bank_transaction,
                        match_date=datetime.now().isoformat(),
                        match_criteria=match_criteria
                    )
                    
                    matches.append(match)
                    matched_platform_transaction_ids.add(pt.get('id'))
                    matched_bank_transaction_ids.add(bank_transaction.get('id'))
        
        # --- Step 2: Process remaining transactions with standard logic ---
        for pt in self._returned_platform_transactions:
            if pt.get('id') in matched_platform_transaction_ids:
                continue

            platform_amount = float(pt.get('Amount', 0))
            platform_date = datetime.fromisoformat(pt.get('Date', '').replace('Z', '+00:00'))
            
            # Find matching bank transactions
            for bt in self._potential_bank_transactions:
                if bt.get('id') in matched_bank_transaction_ids:
                    continue  # Already matched
                
                bank_amount = float(bt.get('amount', 0))
                bank_date = datetime.fromisoformat(bt.get('date', '').replace('Z', '+00:00'))
                
                # Check all matching criteria
                amount_match = self._is_amount_match(platform_amount, bank_amount)
                date_match = self._is_date_match(platform_date, bank_date)
                bank_account_match = self._is_bank_account_match(pt, bt)
                
                # All criteria must match
                if amount_match and date_match and bank_account_match:
                    match = Match(
                        platform_transaction=pt,
                        bank_transaction=bt,
                        match_date=datetime.now().isoformat(),
                        match_criteria=MatchCriteria(
                            amount_match=True,
                            date_match=True,
                            keyword_match=True,
                            user_match=True,
                            is_direct_match=False
                        )
                    )
                    
                    matches.append(match)
                    matched_bank_transaction_ids.add(bt.get('id'))
                    matched_platform_transaction_ids.add(pt.get('id'))
                    break  # Found a match for this platform transaction
        
        # Calculate summary statistics
        total_returned = len(self._returned_platform_transactions)
        total_potential = len(self._potential_bank_transactions)
        total_matches = len(matches)
        match_rate = (total_matches / total_returned * 100) if total_returned > 0 else 0
        
        summary = Summary(
            total_matches=total_matches,
            total_returned_platform_transactions=total_returned,
            total_potential_bank_transactions=total_potential,
            match_rate=match_rate
        )
        
        # Get unmatched transactions
        unmatched_platform_transactions = [
            pt for pt in self._returned_platform_transactions 
            if pt.get('id') not in matched_platform_transaction_ids
        ]
        
        unmatched_bank_transactions = [
            bt for bt in self._potential_bank_transactions 
            if bt.get('id') not in matched_bank_transaction_ids
        ]
        
        return MatchResults(
            matches=matches,
            unmatched_platform_transactions=unmatched_platform_transactions,
            unmatched_bank_transactions=unmatched_bank_transactions,
            summary=summary
        )


# Test function
def test_simple_matching():
    """Test the simple matching logic"""
    
    # Sample platform transaction (returned)
    sample_platform_transaction = {
        "id": 170976,
        "Transaction_Type": "returned",
        "Amount": 4690,
        "Date": "2025-08-05",
        "from": {
            "Account_Name": "Betting bank",
            "Account_Type": "Betting bank account",
            "bankaccount_id": 18668
        },
        "to": {
            "Account_Name": "Checkbook Custodial",
            "Account_Type": "Backer bank account",
            "bankaccount_id": 1313
        }
    }
    
    # Sample bank transaction (potential match)
    sample_bank_transaction = {
        "id": 484560,
        "name": "D15121 INDIVIDUAL 049468200001508 CHECK 5011 KARLA WILLIAMS FRANCIS D15121",
        "amount": 4690,
        "date": "2025-08-05",
        "Account_Type": "EVERYDAY CHECKING ...7034",
        "bankaccount_id": 18668
    }
    
    # Test the matching logic
    matcher = SimpleTransactionMatcher(
        [sample_platform_transaction], 
        [sample_bank_transaction], 
        RETURNED_KEYWORDS
    )
    
    results = matcher.match_returned_transactions()
    
    print("=== Simple Matching Test Results ===")
    print(f"Total matches: {results.summary.total_matches}")
    print(f"Match rate: {results.summary.match_rate:.1f}%")
    
    if results.matches:
        print("\n=== Match Details ===")
        for match in results.matches:
            print(f"Platform Transaction ID: {match.platform_transaction['id']}")
            print(f"Bank Transaction ID: {match.bank_transaction['id']}")
            print(f"Amount: ${match.platform_transaction['Amount']}")
            print(f"Date: {match.platform_transaction['Date']}")
            print(f"Bank Account ID: {match.platform_transaction['from']['bankaccount_id']}")
            print("---")


if __name__ == "__main__":
    test_simple_matching()
