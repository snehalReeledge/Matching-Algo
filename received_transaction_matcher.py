#!/usr/bin/env python3
"""
Simple transaction matcher for received transactions (funds coming in)
Updated to include checkbook payment validation for received transactions.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
import requests
from config import CHECKBOOK_PAYMENTS_API_URL

# API Configuration for checkbook payments
# CHECKBOOK_PAYMENTS_API_URL = "https://xhks-nxia-vlqr.n7c.xano.io/api:1ZwRS-f0/getCheckbookPayments"


@dataclass
class MatchCriteria:
    """Data class for match criteria validation results"""
    amount_match: bool
    date_match: bool
    checkbook_payment_exists: bool
    recipient_match: bool
    direction_match: bool
    description_match: bool
    user_match: bool


@dataclass
class Match:
    """Data class for a matched transaction pair"""
    platform_transaction: Dict[str, Any]
    bank_transaction: Dict[str, Any]
    checkbook_payment: Dict[str, Any]
    match_date: str
    match_criteria: MatchCriteria


@dataclass
class Summary:
    """Data class for matching summary statistics"""
    total_matches: int
    total_received_platform_transactions: int
    total_potential_bank_transactions: int
    total_checkbook_payments: int
    match_rate: float


@dataclass
class MatchResults:
    """Data class for complete matching results"""
    matches: List[Match]
    unmatched_platform_transactions: List[Dict[str, Any]]
    unmatched_bank_transactions: List[Dict[str, Any]]
    unmatched_checkbook_payments: List[Dict[str, Any]]
    summary: Summary


class SimpleReceivedTransactionMatcher:
    """Simple transaction matcher for received transactions (funds coming in) with checkbook validation"""

    def __init__(self, platform_transactions: List[Dict[str, Any]], 
                 bank_transactions: List[Dict[str, Any]], 
                 user_id: int,
                 start_date: Optional[str] = None,
                 end_date: Optional[str] = None):
        """
        Initialize the received transaction matcher
        
        Args:
            platform_transactions: List of platform transaction dictionaries
            bank_transactions: List of bank transaction dictionaries
            user_id: Player ID to match checkbook payments
            start_date: Optional start date for checkbook payments API
            end_date: Optional end date for checkbook payments API
        """
        self.platform_transactions = platform_transactions
        self.bank_transactions = bank_transactions
        self.user_id = user_id
        self.start_date = start_date
        self.end_date = end_date
        
        # Fetch checkbook payments from API
        self.checkbook_payments = self._fetch_checkbook_payments()
        
        # Pre-process data for matching
        self._preprocess_data()

    def _fetch_checkbook_payments(self) -> List[Dict[str, Any]]:
        """Fetch checkbook payments from the API"""
        try:
            # API expects user_id as an array
            params = {
                'start_date': self.start_date,
                'end_date': self.end_date,
                'user_id': [self.user_id]  # API expects array format
            }
            
            response = requests.get(CHECKBOOK_PAYMENTS_API_URL, params=params)
            response.raise_for_status()
            
            data = response.json()
            return data.get('checkbookPayments', [])
            
        except Exception as e:
            print(f"Error fetching checkbook payments: {e}")
            return []

    def _preprocess_data(self):
        """Pre-process data for matching"""
        # Filter received platform transactions (funds coming in)
        self._received_platform_transactions = [
            pt for pt in self.platform_transactions 
            if pt.get('Transaction_Type') == 'received'  # Different from 'returned'
        ]
        
        # Filter bank transactions (negative amounts for received funds)
        # Note: Received funds typically show as negative amounts in bank transactions
        self._potential_bank_transactions = [
            bt for bt in self.bank_transactions
            if bt.get('amount', 0) < 0  # Negative amounts for received funds
        ]
        
        # Filter checkbook payments that match our criteria for received transactions
        self._valid_checkbook_payments = [
            cp for cp in self.checkbook_payments
            if self._is_valid_checkbook_payment_for_received(cp)
        ]

    def _is_valid_checkbook_payment_for_received(self, checkbook_payment: Dict[str, Any]) -> bool:
        """Check if checkbook payment is valid for received transaction matching"""
        # For received transactions, we need to validate the checkbook payment exists
        # Must have valid amount
        if not checkbook_payment.get('amount'):
            return False
        
        # Must have valid recipient
        if not checkbook_payment.get('recipient'):
            return False
        
        # For received transactions, the checkbook payment should typically be OUTGOING
        # since funds are coming INTO the platform from the checkbook
        if checkbook_payment.get('direction') != 'OUTGOING':
            return False
        
        # Check description validation: either contains "fund" (case-insensitive) OR starts with "F" (case-insensitive)
        description = checkbook_payment.get('description', '').lower()
        if not ('fund' in description or description.startswith('f')):
            return False
        
        return True

    def _find_matching_checkbook_payment(self, bank_transaction: Dict[str, Any], platform_transaction: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Find matching checkbook payment for a bank transaction"""
        bank_amount = abs(float(bank_transaction.get('amount', 0)))
        bank_date = datetime.fromisoformat(bank_transaction.get('date', '').replace('Z', '+00:00'))
        
        # Also get platform date for better checkbook payment selection
        platform_date = datetime.fromisoformat(platform_transaction.get('Date', '').replace('Z', '+00:00'))
        
        matching_payments = []
        
        for cp in self._valid_checkbook_payments:
            # Check if recipient matches user_id
            if cp.get('recipient') != self.user_id:
                continue
            
            # Check if direction is OUTGOING (for received transactions)
            if cp.get('direction') != 'OUTGOING':
                continue
            
            # Check if amounts match (within $0.01 tolerance)
            checkbook_amount = float(cp.get('amount', 0))
            if abs(bank_amount - checkbook_amount) <= 0.01:
                # Check description validation: either contains "fund" (case-insensitive) OR starts with "F"
                description = cp.get('description', '').lower()
                if 'fund' in description or description.startswith('f'):
                    # Check date match: bank transaction date vs checkbook payment date (±2 days)
                    # This validates that the checkbook payment corresponds to the specific bank transaction
                    try:
                        checkbook_timestamp = cp.get('date', 0)
                        if checkbook_timestamp:
                            checkbook_date = datetime.fromtimestamp(checkbook_timestamp / 1000)
                            # Use bank date for comparison (as requested: bank transaction vs checkbook payment)
                            date_diff = abs((bank_date - checkbook_date).total_seconds())
                            # Allow ±3 days tolerance (259,200 seconds = 3 days) - UPDATED
                            if date_diff <= 259200:
                                matching_payments.append(cp)
                    except (ValueError, TypeError):
                        # If date parsing fails, skip this payment
                        continue
        
        if not matching_payments:
            return None
        
        # If only one matching payment, return it
        if len(matching_payments) == 1:
            return matching_payments[0]
        
        # If multiple matching payments, prioritize same-date matches, then closest time
        best_payment = None
        smallest_date_diff = None
        
        # First, try to find exact date matches
        same_date_payments = []
        for cp in matching_payments:
            try:
                checkbook_timestamp = cp.get('date', 0)
                if checkbook_timestamp:
                    checkbook_date = datetime.fromtimestamp(checkbook_timestamp / 1000)
                    # Check if dates are the same (ignoring time)
                    if platform_date.date() == checkbook_date.date():
                        same_date_payments.append(cp)
            except (ValueError, TypeError):
                continue
        
        # If we have same-date payments, find the closest time within that date
        if same_date_payments:
            for cp in same_date_payments:
                try:
                    checkbook_timestamp = cp.get('date', 0)
                    if checkbook_timestamp:
                        checkbook_date = datetime.fromtimestamp(checkbook_timestamp / 1000)
                        # Compare time within the same date
                        time_diff = abs((bank_date - checkbook_date).total_seconds())
                        
                        if smallest_date_diff is None or time_diff < smallest_date_diff:
                            smallest_date_diff = time_diff
                            best_payment = cp
                except (ValueError, TypeError):
                    continue
        else:
            # No same-date payments, fall back to closest date match
            for cp in matching_payments:
                try:
                    checkbook_timestamp = cp.get('date', 0)
                    if checkbook_timestamp:
                        checkbook_date = datetime.fromtimestamp(checkbook_timestamp / 1000)
                        date_diff = abs((bank_date - checkbook_date).total_seconds())
                        
                        if smallest_date_diff is None or date_diff < smallest_date_diff:
                            smallest_date_diff = date_diff
                            best_payment = cp
                except (ValueError, TypeError):
                    continue
        
        # Always return the best date-based match, never fallback to first
        if best_payment:
            return best_payment
        
        # If no date-based match found, return None instead of arbitrary first
        return None

    def _is_amount_match(self, platform_amount: float, bank_amount: float) -> bool:
        """Check if amounts match within $0.01 tolerance"""
        # For received transactions, platform amount should be positive, bank amount negative
        # So we compare the absolute values
        return abs(abs(platform_amount) - abs(bank_amount)) <= 0.01

    def _is_date_match(self, platform_date: datetime, bank_date: datetime) -> bool:
        """Check if dates are within 5 days of each other (core matching tolerance)"""
        date_diff = abs(platform_date - bank_date)
        return date_diff <= timedelta(days=5)

    def _is_bank_account_match(self, platform_transaction: Dict[str, Any], 
                              bank_transaction: Dict[str, Any]) -> bool:
        """Check if bank account IDs match"""
        # For received transactions, check the 'to' account (where funds are going)
        platform_to_account = platform_transaction.get('to', {})
        platform_bank_account_id = platform_to_account.get('bankaccount_id')
        bank_account_id = bank_transaction.get('bankaccount_id')
        
        return platform_bank_account_id == bank_account_id

    def match_received_transactions(self) -> MatchResults:
        """
        Match received platform transactions with bank transactions using checkbook payment validation
        
        Returns:
            MatchResults object containing all matches and statistics
        """
        matches = []
        matched_bank_transaction_ids = set()
        matched_platform_transaction_ids = set()
        matched_checkbook_payment_ids = set()
        
        # Match each received platform transaction
        for pt in self._received_platform_transactions:
            platform_amount = float(pt.get('Amount', 0))
            platform_date = datetime.fromisoformat(pt.get('Date', '').replace('Z', '+00:00'))
            
            # Find matching bank transactions with date prioritization
            matching_bank_transactions = []
            
            for bt in self._potential_bank_transactions:
                if bt.get('id') in matched_bank_transaction_ids:
                    continue  # Already matched
                
                bank_amount = float(bt.get('amount', 0))
                bank_date = datetime.fromisoformat(bt.get('date', '').replace('Z', '+00:00'))
                
                # Check basic matching criteria
                amount_match = self._is_amount_match(platform_amount, bank_amount)
                date_match = self._is_date_match(platform_date, bank_date)
                bank_account_match = self._is_bank_account_match(pt, bt)
                
                # Find matching checkbook payment
                checkbook_payment = self._find_matching_checkbook_payment(bt, pt)
                checkbook_payment_exists = checkbook_payment is not None
                
                # All criteria must match
                if amount_match and date_match and bank_account_match and checkbook_payment_exists:
                    # Check if checkbook payment is already matched
                    if checkbook_payment.get('id') in matched_checkbook_payment_ids:
                        continue
                    
                    # Calculate date difference for prioritization
                    date_diff = abs(platform_date - bank_date)
                    is_exact_date_match = date_diff.days == 0
                    
                    matching_bank_transactions.append({
                        'bank_transaction': bt,
                        'checkbook_payment': checkbook_payment,
                        'date_diff': date_diff,
                        'is_exact_date_match': is_exact_date_match
                    })
            
            # If we found matching bank transactions, select the best one
            if matching_bank_transactions:
                # Prioritize exact date matches, then closest date
                exact_date_matches = [m for m in matching_bank_transactions if m['is_exact_date_match']]
                
                if exact_date_matches:
                    # Use exact date match with closest time
                    best_match = min(exact_date_matches, key=lambda m: m['date_diff'])
                else:
                    # No exact date match, use closest date
                    best_match = min(matching_bank_transactions, key=lambda m: m['date_diff'])
                
                bt = best_match['bank_transaction']
                checkbook_payment = best_match['checkbook_payment']
                
                # Create match criteria
                match_criteria = MatchCriteria(
                    amount_match=True,
                    date_match=True,
                    checkbook_payment_exists=True,
                    recipient_match=checkbook_payment.get('recipient') == self.user_id,
                    direction_match=checkbook_payment.get('direction') == 'OUTGOING',
                    description_match=self._is_valid_checkbook_payment_for_received(checkbook_payment),
                    user_match=True
                )
                
                match = Match(
                    platform_transaction=pt,
                    bank_transaction=bt,
                    checkbook_payment=checkbook_payment,
                    match_date=datetime.now().isoformat(),
                    match_criteria=match_criteria
                )
                
                matches.append(match)
                matched_bank_transaction_ids.add(bt.get('id'))
                matched_platform_transaction_ids.add(pt.get('id')) # Ensure this is always added
                matched_checkbook_payment_ids.add(checkbook_payment.get('id'))
        
        # Calculate summary statistics
        total_received = len(self._received_platform_transactions)
        total_potential = len(self._potential_bank_transactions)
        total_checkbook_payments = len(self._valid_checkbook_payments)
        total_matches = len(matches)
        match_rate = (total_matches / total_received * 100) if total_received > 0 else 0
        
        summary = Summary(
            total_matches=total_matches,
            total_received_platform_transactions=total_received,
            total_potential_bank_transactions=total_potential,
            total_checkbook_payments=total_checkbook_payments,
            match_rate=match_rate
        )
        
        # Get unmatched transactions
        unmatched_platform_transactions = [
            pt for pt in self._received_platform_transactions 
            if pt.get('id') not in matched_platform_transaction_ids
        ]
        
        unmatched_bank_transactions = [
            bt for bt in self._potential_bank_transactions 
            if bt.get('id') not in matched_bank_transaction_ids
        ]
        
        unmatched_checkbook_payments = [
            cp for cp in self._valid_checkbook_payments 
            if cp.get('id') not in matched_checkbook_payment_ids
        ]
        
        return MatchResults(
            matches=matches,
            unmatched_platform_transactions=unmatched_platform_transactions,
            unmatched_bank_transactions=unmatched_bank_transactions,
            unmatched_checkbook_payments=unmatched_checkbook_payments,
            summary=summary
        )


# Test function for received transactions with checkbook validation
def test_received_matching():
    """Test the received transaction matching logic with sample data"""
    
    # Sample platform transaction (received - funds coming in)
    sample_platform_transaction = {
        "id": 170977,
        "player_id": 15121,
        "Transaction_Type": "received",
        "Amount": 2500,
        "Date": "2025-08-06",
        "from": {
            "Account_Name": "External Source",
            "Account_Type": "External account",
            "bankaccount_id": 99999
        },
        "to": {
            "Account_Name": "Player 15121 Account",
            "Account_Type": "Player bank account",
            "bankaccount_id": 18668
        }
    }
    
    # Sample bank transaction (negative amount for received funds)
    sample_bank_transaction = {
        "id": 484561,
        "player_id": 15121,
        "name": "DEPOSIT FROM EXTERNAL SOURCE TRANSFER 12345",
        "amount": -2500,  # Negative amount for received funds
        "date": "2025-08-06",
        "Account_Type": "EVERYDAY CHECKING ...7034",
        "bankaccount_id": 18668
    }
    
    # Sample checkbook payment (would come from API)
    sample_checkbook_payment = {
        "id": 17720,
        "name": "Player 15121",
        "uid": "checkbook-uid-001",
        "status": "PAID",
        "amount": 2500,
        "created_at": 1755573896460,
        "date": 1755573896460,
        "description": "Deposit from external source",
                    "direction": "OUTGOING",  # Must be OUTGOING for received transactions
        "image_uri": "https://checkbook-checks.s3.amazonaws.com/test.png",
        "number": "16797",
        "recipient_email": "player15121@test.com",
        "sender_email": "external@source.com",
        "recipient": 15121,  # Must match user_id
        "sender": 99999,
        "actioned": False,
        "group": "deposit",
        "last_updated": 1755573896460,
        "left_bank": False,
        "error_code": "",
        "check_id": ""
    }
    
    print("=== Received Transaction Matching Test (with Checkbook Validation) ===")
    print("Note: This test uses sample data. In production, checkbook payments come from API.")
    print(f"API Input Format: {{'start_date': null, 'end_date': null, 'user_id': [15121]}}")
    
    # Test the received transaction matching logic
    matcher = SimpleReceivedTransactionMatcher(
        [sample_platform_transaction], 
        [sample_bank_transaction],
        user_id=15121
    )
    
    # Manually add the sample checkbook payment for testing
    matcher.checkbook_payments = [sample_checkbook_payment]
    matcher._preprocess_data()
    
    results = matcher.match_received_transactions()
    
    print(f"\n=== Received Transaction Matching Results ===")
    print(f"Total Received Platform Transactions: {results.summary.total_received_platform_transactions}")
    print(f"Total Potential Bank Transactions: {results.summary.total_potential_bank_transactions}")
    print(f"Total Valid Checkbook Payments: {results.summary.total_checkbook_payments}")
    print(f"Total Matches Found: {results.summary.total_matches}")
    print(f"Match Rate: {results.summary.match_rate:.1f}%")
    
    if results.matches:
        print(f"\n=== Match Details ===")
        for match in results.matches:
            print(f"Platform Transaction ID: {match.platform_transaction['id']}")
            print(f"Bank Transaction ID: {match.bank_transaction['id']}")
            print(f"Checkbook Payment ID: {match.checkbook_payment['id']}")
            print(f"Amount: ${match.platform_transaction['Amount']}")
            print(f"Date: {match.platform_transaction['Date']}")
            print(f"Checkbook Description: {match.checkbook_payment['description']}")
            print(f"Direction: {match.checkbook_payment['direction']}")
            print(f"Recipient: {match.checkbook_payment['recipient']}")
            print("---")
    else:
        print("\nNo matches found. This might indicate:")
        print("1. No valid checkbook payments exist")
        print("2. Amount mismatch")
        print("3. Date mismatch")
        print("4. Bank account ID mismatch")
        print("5. Checkbook payment criteria not met")


if __name__ == "__main__":
    test_received_matching()
