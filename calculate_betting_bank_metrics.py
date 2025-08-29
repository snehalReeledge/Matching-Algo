import json
import requests
import os
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# Import from existing modules
from config import (
    PLAYERS_API_URL,
    PLATFORM_TRANSACTIONS_API_URL,
    BANK_TRANSACTIONS_API_URL,
    RETURNED_KEYWORDS
)
from received_transaction_matcher import SimpleReceivedTransactionMatcher
from returned_transaction_matcher import SimpleTransactionMatcher as ReturnedTransactionMatcher

def fetch_pro_players():
    """Fetches all 'Pro' players, excluding internal accounts."""
    print("Fetching 'Pro' players (excluding @reeledge.com)...")
    try:
        response = requests.get(PLAYERS_API_URL, timeout=60)
        response.raise_for_status()
        pro_players = [p for p in response.json() if p.get('player_stage') == 'Pro']
        filtered_players = [p for p in pro_players if '@reeledge.com' not in p.get('email', '').lower()]
        print(f"Found {len(filtered_players)} 'Pro' players to analyze.")
        return filtered_players
    except requests.exceptions.RequestException as e:
        print(f"Fatal: Could not fetch players. {e}")
        return []

def analyze_player_for_betting_metrics(player, all_platform_transactions, date_ranges):
    """Analyzes a single player's data to extract betting bank account metrics."""
    player_id = player.get('id')
    
    try:
        player_platform_transactions = [pt for pt in all_platform_transactions if pt.get('User_ID') == player_id]
        
        bank_response = requests.get(
            BANK_TRANSACTIONS_API_URL,
            params={'player_id': player_id, 'start_date': date_ranges['bank_start'], 'end_date': date_ranges['bank_end']},
            timeout=60
        )
        bank_response.raise_for_status()
        player_bank_transactions = bank_response.json().get('bankTransactions', [])

        # Run matchers to get the list of successful matches
        received_matcher = SimpleReceivedTransactionMatcher(player_platform_transactions, player_bank_transactions, player_id, date_ranges['checkbook_start'], date_ranges['checkbook_end'])
        received_results = received_matcher.match_received_transactions()
        
        returned_matcher = ReturnedTransactionMatcher(player_platform_transactions, player_bank_transactions, RETURNED_KEYWORDS)
        returned_results = returned_matcher.match_returned_transactions()

        # --- METRICS CALCULATION ---
        # 1. Total Betting Bank Transactions (from Platform data)
        total_betting_received = sum(1 for pt in player_platform_transactions if pt.get('Transaction_Type') == 'received' and 'betting' in pt.get('to', {}).get('Account_Type', '').lower())
        total_betting_returned = sum(1 for pt in player_platform_transactions if pt.get('Transaction_Type') == 'returned' and 'betting' in pt.get('from', {}).get('Account_Type', '').lower())
        
        # 2. Matched Betting Bank Transactions
        matched_betting_received = sum(1 for match in received_results.matches if 'betting' in match.platform_transaction.get('to', {}).get('Account_Type', '').lower())
        matched_betting_returned = sum(1 for match in returned_results.matches if 'betting' in match.platform_transaction.get('from', {}).get('Account_Type', '').lower())

        return {
            "total_betting_received": total_betting_received,
            "matched_betting_received": matched_betting_received,
            "total_betting_returned": total_betting_returned,
            "matched_betting_returned": matched_betting_returned
        }

    except requests.exceptions.RequestException:
        return None # Skip players with API errors

def calculate_betting_bank_metrics(num_players, start_date_str, end_date_str):
    """Orchestrates the analysis to calculate and display betting bank account match rates."""
    
    # Calculate date ranges
    date_format = "%Y-%m-%d"
    start_date = datetime.strptime(start_date_str, date_format)
    end_date = datetime.strptime(end_date_str, date_format)
    date_ranges = {
        'bank_start': (start_date - timedelta(days=5)).strftime(date_format),
        'bank_end': (end_date + timedelta(days=5)).strftime(date_format),
        'checkbook_start': (start_date - timedelta(days=7)).strftime(date_format),
        'checkbook_end': (end_date + timedelta(days=7)).strftime(date_format)
    }

    pro_players = fetch_pro_players()
    if not pro_players: return

    print("Fetching all platform transactions...")
    pt_response = requests.get(PLATFORM_TRANSACTIONS_API_URL, params={'start_date': start_date_str, 'end_date': end_date_str}, timeout=120)
    all_platform_transactions = pt_response.json()
    
    players_to_analyze = pro_players[:num_players]
    
    # Aggregate metrics
    total_metrics = {
        "total_betting_received": 0, "matched_betting_received": 0,
        "total_betting_returned": 0, "matched_betting_returned": 0
    }

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(analyze_player_for_betting_metrics, player, all_platform_transactions, date_ranges) for player in players_to_analyze]
        for future in as_completed(futures):
            result = future.result()
            if result:
                for key in total_metrics:
                    total_metrics[key] += result[key]

    # Calculate and display the final percentages
    received_match_rate = (total_metrics['matched_betting_received'] / total_metrics['total_betting_received'] * 100) if total_metrics['total_betting_received'] > 0 else 0
    returned_match_rate = (total_metrics['matched_betting_returned'] / total_metrics['total_betting_returned'] * 100) if total_metrics['total_betting_returned'] > 0 else 0
    
    print("\n" + "="*50)
    print("Betting Bank Account Matching Metrics")
    print("="*50)
    print(f"\n--- Received Transactions (Betting Bank) ---")
    print(f"  - Total Transactions: {total_metrics['total_betting_received']}")
    print(f"  - Matched Transactions: {total_metrics['matched_betting_received']}")
    print(f"  - Match Rate: {received_match_rate:.2f}%")
    
    print(f"\n--- Returned Transactions (Betting Bank) ---")
    print(f"  - Total Transactions: {total_metrics['total_betting_returned']}")
    print(f"  - Matched Transactions: {total_metrics['matched_betting_returned']}")
    print(f"  - Match Rate: {returned_match_rate:.2f}%")
    print("\n" + "="*50)

if __name__ == "__main__":
    NUM_PLAYERS_TO_ANALYZE = 50
    START_DATE = "2025-07-01"
    END_DATE = "2025-07-31"
    calculate_betting_bank_metrics(NUM_PLAYERS_TO_ANALYZE, START_DATE, END_DATE)

