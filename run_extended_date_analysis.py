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
    """Fetches all players from the API and filters for those at the 'Pro' stage, excluding internal accounts."""
    print("Fetching all 'Pro' players (excluding @reeledge.com accounts)...")
    try:
        response = requests.get(PLAYERS_API_URL, timeout=60)
        response.raise_for_status()
        all_players = response.json()
        
        # First, filter for Pro players
        pro_players = [p for p in all_players if p.get('player_stage') == 'Pro']
        
        # Next, filter out any internal accounts based on email
        filtered_players = [
            p for p in pro_players 
            if '@reeledge.com' not in p.get('email', '').lower()
        ]
        
        print(f"Found {len(filtered_players)} 'Pro' players after filtering {len(pro_players) - len(filtered_players)} internal accounts.")
        return filtered_players
    except requests.exceptions.RequestException as e:
        print(f"Fatal: Could not fetch players from API. Error: {e}")
        return []

def fetch_all_platform_transactions(start_date_str, end_date_str):
    """Fetches all platform transactions for the STRICT date range."""
    print(f"Fetching platform transactions from {start_date_str} to {end_date_str}...")
    try:
        response = requests.get(
            PLATFORM_TRANSACTIONS_API_URL,
            params={'start_date': start_date_str, 'end_date': end_date_str},
            timeout=120
        )
        response.raise_for_status()
        platform_transactions = response.json()
        print(f"Found {len(platform_transactions)} total platform transactions.")
        return platform_transactions
    except requests.exceptions.RequestException as e:
        print(f"Fatal: Could not fetch platform transactions. Error: {e}")
        return []

def analyze_single_player(player, all_platform_transactions, bank_start_date_str, bank_end_date_str, checkbook_start_date_str, checkbook_end_date_str):
    """
    Analyzes transactions for a single player using extended date ranges for bank and checkbook data.
    """
    player_id = player.get('id')
    player_name = player.get('name', 'Unknown Player')
    print(f"Processing player {player_id} ({player_name})...")

    try:
        player_platform_transactions = [
            pt for pt in all_platform_transactions if pt.get('User_ID') == player_id
        ]

        bank_response = requests.get(
            BANK_TRANSACTIONS_API_URL,
            params={'player_id': player_id, 'start_date': bank_start_date_str, 'end_date': bank_end_date_str},
            timeout=60
        )
        bank_response.raise_for_status()
        player_bank_transactions = bank_response.json().get('bankTransactions', [])

        received_matcher = SimpleReceivedTransactionMatcher(
            platform_transactions=player_platform_transactions,
            bank_transactions=player_bank_transactions,
            user_id=player_id,
            start_date=checkbook_start_date_str,
            end_date=checkbook_end_date_str
        )
        received_results = received_matcher.match_received_transactions()

        returned_matcher = ReturnedTransactionMatcher(
            platform_transactions=player_platform_transactions,
            bank_transactions=player_bank_transactions,
            returned_keywords=RETURNED_KEYWORDS
        )
        returned_results = returned_matcher.match_returned_transactions()

        return {
            "player_id": player_id,
            "player_name": player_name,
            "platform_transactions_count": len(player_platform_transactions),
            "bank_transactions_count": len(player_bank_transactions),
            "received_matches": len(received_results.matches),
            "returned_matches": len(returned_results.matches),
            "unmatched_received_count": len(received_results.unmatched_platform_transactions),
            "unmatched_returned_count": len(returned_results.unmatched_platform_transactions),
        }

    except requests.exceptions.RequestException as e:
        print(f"Error processing player {player_id} ({player_name}): {e}")
        return {"player_id": player_id, "player_name": player_name, "error": str(e)}

def run_extended_date_analysis(num_players, start_date_str, end_date_str):
    """
    Orchestrates the analysis for a batch of players with extended date ranges.
    """
    # Calculate the extended date ranges
    date_format = "%Y-%m-%d"
    start_date = datetime.strptime(start_date_str, date_format)
    end_date = datetime.strptime(end_date_str, date_format)

    bank_start_date = start_date - timedelta(days=5)
    bank_end_date = end_date + timedelta(days=5)
    checkbook_start_date = start_date - timedelta(days=7)
    checkbook_end_date = end_date + timedelta(days=7)

    bank_start_date_str = bank_start_date.strftime(date_format)
    bank_end_date_str = bank_end_date.strftime(date_format)
    checkbook_start_date_str = checkbook_start_date.strftime(date_format)
    checkbook_end_date_str = checkbook_end_date.strftime(date_format)

    print("="*60)
    print("Starting Analysis with Extended Date Ranges")
    print("="*60)
    print(f"Platform Transaction Dates: {start_date_str} to {end_date_str}")
    print(f"Bank Transaction Dates:     {bank_start_date_str} to {bank_end_date_str} (±5 days)")
    print(f"Checkbook Payment Dates:    {checkbook_start_date_str} to {checkbook_end_date_str} (±7 days)")
    print("="*60)

    pro_players = fetch_pro_players()
    if not pro_players: return

    all_platform_transactions = fetch_all_platform_transactions(start_date_str, end_date_str)
    if not all_platform_transactions: return

    players_to_analyze = pro_players[:num_players]
    print(f"\nStarting analysis for the first {len(players_to_analyze)} 'Pro' players...")

    all_results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_player = {
            executor.submit(analyze_single_player, player, all_platform_transactions, bank_start_date_str, bank_end_date_str, checkbook_start_date_str, checkbook_end_date_str): player
            for player in players_to_analyze
        }
        for future in as_completed(future_to_player):
            player_result = future.result()
            if player_result:
                all_results.append(player_result)

    all_results.sort(key=lambda x: x.get('player_id', 0))

    output_dir = "debug"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    output_filename = os.path.join(output_dir, f"extended_date_analysis_{num_players}_players_no_internal.json")
    with open(output_filename, 'w') as f:
        json.dump(all_results, f, indent=4)
        
    print(f"\nBatch analysis complete. Results saved to '{output_filename}'")

if __name__ == "__main__":
    NUM_PLAYERS_TO_ANALYZE = 50
    START_DATE = "2025-07-01"
    END_DATE = "2025-07-31"  # Changed to 31st for a full July
    run_extended_date_analysis(NUM_PLAYERS_TO_ANALYZE, START_DATE, END_DATE)
