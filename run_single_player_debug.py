import json
import requests
import os
import time
import argparse
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

from update_bank_transaction import update_bank_transaction
from update_platform_transaction import update_platform_transaction_date

# Import from existing modules
from config import (
    PLATFORM_TRANSACTIONS_API_URL,
    BANK_TRANSACTIONS_API_URL,
    RETURNED_KEYWORDS,
    PLAYERS_API_URL
)
from received_transaction_matcher import SimpleReceivedTransactionMatcher
from returned_transaction_matcher import SimpleTransactionMatcher as ReturnedTransactionMatcher

def fetch_pro_players():
    """Fetches all 'Pre-offboarded' players, excluding internal accounts."""
    print("Fetching 'Pre-offboarded' players (excluding @reeledge.com)...")
    try:
        response = requests.get(PLAYERS_API_URL, timeout=60)
        response.raise_for_status()
        pro_players = [p for p in response.json() if p.get('player_stage') == 'Pre-offboarded']
        filtered_players = [p for p in pro_players if '@reeledge.com' not in p.get('email', '').lower()]
        print(f"Found {len(filtered_players)} 'Pre-offboarded' players to analyze.")
        return filtered_players
    except requests.exceptions.RequestException as e:
        print(f"Fatal: Could not fetch players. {e}")
        return []

def analyze_player_transactions(player_id, start_date_str, end_date_str, date_ranges):
    """Runs the main matching logic for a single player and returns the matcher instances."""
    try:
        pt_response = requests.get(PLATFORM_TRANSACTIONS_API_URL, params={'user_id': player_id, 'start_date': start_date_str, 'end_date': end_date_str}, timeout=120)
        pt_response.raise_for_status()
        platform_transactions = pt_response.json()

        # Sort platform transactions by date in ascending order
        platform_transactions.sort(key=lambda x: x.get('Date', ''))

        bank_response = requests.get(BANK_TRANSACTIONS_API_URL, params={'player_id': player_id, 'start_date': date_ranges['bank_start'], 'end_date': date_ranges['bank_end']}, timeout=60)
        bank_response.raise_for_status()
        bank_transactions = bank_response.json().get('bankTransactions', [])

        received_matcher = SimpleReceivedTransactionMatcher(platform_transactions, bank_transactions, player_id, date_ranges['checkbook_start'], date_ranges['checkbook_end'])
        received_results = received_matcher.match_received_transactions()

        returned_matcher = ReturnedTransactionMatcher(platform_transactions, bank_transactions, RETURNED_KEYWORDS)
        returned_results = returned_matcher.match_returned_transactions()
        
        return received_matcher, returned_matcher, platform_transactions, received_results, returned_results
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for player {player_id}: {e}")
        return None, None, None, None, None

def investigate_unmatched_received(player_id, matcher, match_results):
    """Generates detailed reasons for unmatched received betting bank transactions."""
    findings = []
    unmatched_betting_pts = [
        pt for pt in match_results.unmatched_platform_transactions
        if 'betting bank account' in pt.get('to', {}).get('Account_Type', '').lower()
        and not pt.get('related_bank_transaction') # Exclude PTs that already have a bank link
    ]

    for unmatched_pt in unmatched_betting_pts:
        reason = "Unknown reason."
        # Logic to find the reason (copied and adapted from investigate script)
        potential_bank_matches = []
        for bt in matcher._potential_bank_transactions:
            if matcher._is_amount_match(float(unmatched_pt.get('Amount', 0)), float(bt.get('amount', 0))) and \
                matcher._is_date_match(datetime.fromisoformat(unmatched_pt.get('Date', '').replace('Z', '+00:00')), datetime.fromisoformat(bt.get('date', '').replace('Z', '+00:00'))) and \
                matcher._is_bank_account_match(unmatched_pt, bt):
                potential_bank_matches.append(bt)

        if not potential_bank_matches:
            reason = "No suitable Bank Transaction found (failed amount, date, or account ID check)."
        else:
            found_match_with_used_cp = False
            for pot_bt in potential_bank_matches:
                # Temporarily bypass the 'used' check to see what would have happened
                checkbook_payment = matcher._find_matching_checkbook_payment(pot_bt, unmatched_pt, bypass_used_check=True)
                if checkbook_payment:
                    cp_id = checkbook_payment.get('id')
                    if cp_id in matcher.used_checkbook_payment_ids:
                        conflicting_pt_id = matcher.used_checkbook_payment_ids[cp_id]
                        reason = f"A valid checkbook payment (ID: {cp_id}) was found, but it was already used by Platform Transaction ID {conflicting_pt_id}."
                        found_match_with_used_cp = True
                        break
            if not found_match_with_used_cp:
                reason = "A potential Bank Transaction was found, but no valid Checkbook Payment could be matched to it."

        findings.append({
            "platform_transaction_id": unmatched_pt.get('id'),
            "platform_transaction_amount": unmatched_pt.get('Amount'),
            "platform_transaction_date": unmatched_pt.get('Date'),
            "from_account_type": unmatched_pt.get('from', {}).get('Account_Type'),
            "to_account_type": unmatched_pt.get('to', {}).get('Account_Type'),
            "reason_for_no_match": reason
        })
    return findings

def calculate_metrics(matcher_received, matcher_returned, platform_transactions, received_results, returned_results):
    """Calculates and returns the matching percentages for betting bank accounts."""
    total_betting_received = sum(1 for pt in platform_transactions if pt.get('Transaction_Type') == 'received' and 'betting' in pt.get('to', {}).get('Account_Type', '').lower())
    matched_betting_received = sum(1 for match in received_results.matches if 'betting' in match.platform_transaction.get('to', {}).get('Account_Type', '').lower())
    
    total_betting_returned = sum(1 for pt in platform_transactions if pt.get('Transaction_Type') == 'returned' and 'betting' in pt.get('from', {}).get('Account_Type', '').lower())
    matched_betting_returned = sum(1 for match in returned_results.matches if 'betting' in match.platform_transaction.get('from', {}).get('Account_Type', '').lower())

    received_rate = (matched_betting_received / total_betting_received * 100) if total_betting_received > 0 else 100
    returned_rate = (matched_betting_returned / total_betting_returned * 100) if total_betting_returned > 0 else 100
    
    return {
        "received": {"total": total_betting_received, "matched": matched_betting_received, "rate": f"{received_rate:.2f}%"},
        "returned": {"total": total_betting_returned, "matched": matched_betting_returned, "rate": f"{returned_rate:.2f}%"}
    }


def run_single_player_debug(player_id, start_date_str, end_date_str):
    """Orchestrates the entire debugging workflow for a single player."""
    
    # 1. Setup
    output_dir = f"debug/player_{player_id}_investigation"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    print("="*60)
    print(f"Starting Debugging for Player ID: {player_id}")
    if start_date_str and end_date_str:
        print(f"Date Range: {start_date_str} to {end_date_str}")
        date_format = "%Y-%m-%d"
        start_date = datetime.strptime(start_date_str, date_format)
        end_date = datetime.strptime(end_date_str, date_format)
        date_ranges = {
            'bank_start': (start_date - timedelta(days=5)).strftime(date_format),
            'bank_end': (end_date + timedelta(days=5)).strftime(date_format),
            'checkbook_start': (start_date - timedelta(days=7)).strftime(date_format),
            'checkbook_end': (end_date + timedelta(days=7)).strftime(date_format)
        }
    else:
        print("Date Range: All time")
        start_date_str = None
        end_date_str = None
        date_ranges = {
            'bank_start': None, 'bank_end': None,
            'checkbook_start': None, 'checkbook_end': None
        }

    print(f"Output will be saved in: {output_dir}")
    print("="*60)

    # 2. Run Analysis
    print("\n[Step 1/5] Running transaction matching analysis...")
    received_matcher, returned_matcher, platform_transactions, received_results, returned_results = analyze_player_transactions(player_id, start_date_str, end_date_str, date_ranges)
    if not received_matcher:
        return
    print(" -> Analysis complete.")

    # Dump the raw platform transactions for inspection
    output_pt_dump_file = os.path.join(output_dir, "platform_transactions_dump.json")
    with open(output_pt_dump_file, 'w') as f:
        json.dump(platform_transactions, f, indent=4)

    # 3. Run Investigation
    print("\n[Step 2/5] Investigating unmatched betting bank transactions...")
    unmatched_reasons = investigate_unmatched_received(player_id, received_matcher, received_results)
    # (Could add returned investigation here if needed)
    
    output_investigation_file = os.path.join(output_dir, "unmatched_betting_bank_reasons.json")
    with open(output_investigation_file, 'w') as f:
        json.dump(unmatched_reasons, f, indent=4)
    print(f" -> Investigation complete. Found {len(unmatched_reasons)} unmatched. Results saved.")

    # 4. Calculate Metrics
    print("\n[Step 3/5] Calculating betting bank matching percentages...")
    metrics = calculate_metrics(received_matcher, returned_matcher, platform_transactions, received_results, returned_results)
    
    output_metrics_file = os.path.join(output_dir, "matching_summary.txt")
    with open(output_metrics_file, 'w') as f:
        f.write("--- Betting Bank Matching Metrics ---\n")
        f.write(f"Received: {metrics['received']['matched']}/{metrics['received']['total']} ({metrics['received']['rate']})\n")
        f.write(f"Returned: {metrics['returned']['matched']}/{metrics['returned']['total']} ({metrics['returned']['rate']})\n")
    print(" -> Metrics calculation complete. Results saved.")
    
    # 5. Update Bank Transaction Links
    print("\n[Step 4/5] Updating bank transaction links for matched items...")
    updated_count = 0
    for match in received_results.matches:
        bank_transaction_id = match.bank_transaction.get('transaction_id') # Changed from 'id'
        platform_id = match.platform_transaction.get('id')
        if bank_transaction_id and platform_id:
            update_bank_transaction(bank_transaction_id, platform_id)
            updated_count += 1
            
    for match in returned_results.matches:
        bank_transaction_id = match.bank_transaction.get('transaction_id') # Changed from 'id'
        platform_id = match.platform_transaction.get('id')
        if bank_transaction_id and platform_id:
            update_bank_transaction(bank_transaction_id, platform_id)
            updated_count += 1
    print(f" -> Update requests sent for {updated_count} matched transactions.")

    # 6. Update Platform Transaction Dates
    print("\n[Step 5/6] Syncing dates for matched transactions with different dates...")
    dates_synced_count = 0
    all_matches = received_results.matches + returned_results.matches
    for match in all_matches:
        platform_date_str = match.platform_transaction.get('Date', '').split('T')[0]
        bank_date_str = match.bank_transaction.get('date', '').split('T')[0]

        if platform_date_str and bank_date_str and platform_date_str != bank_date_str:
            platform_id = match.platform_transaction.get('id')
            bank_date = match.bank_transaction.get('date')
            if platform_id and bank_date:
                update_platform_transaction_date(platform_id, bank_date)
                dates_synced_count += 1
    print(f" -> Date sync requests sent for {dates_synced_count} transactions.")


    # 7. Save Matched Pairs
    print("\n[Step 6/6] Saving all matched transaction pairs...")

    def format_matches(matches, match_type):
        formatted = []
        for match in matches:
            data = {
                "platform_transaction_id": match.platform_transaction.get('id'),
                "platform_transaction_amount": match.platform_transaction.get('Amount'),
                "platform_transaction_date": match.platform_transaction.get('Date'),
                "bank_transaction_id": match.bank_transaction.get('id'),
                "bank_transaction_amount": match.bank_transaction.get('amount'),
                "bank_transaction_date": match.bank_transaction.get('date')
            }
            # Only received matches have checkbook payments
            if match_type == 'received' and hasattr(match, 'checkbook_payment') and match.checkbook_payment:
                data["checkbook_payment_id"] = match.checkbook_payment.get('id')
            formatted.append(data)
        return formatted

    all_matches = {
        "received_matches": format_matches(received_results.matches, 'received'),
        "returned_matches": format_matches(returned_results.matches, 'returned')
    }
    
    output_matches_file = os.path.join(output_dir, "matched_pairs.json")
    with open(output_matches_file, 'w') as f:
        json.dump(all_matches, f, indent=4)
    print(f" -> Saved {len(received_results.matches)} received and {len(returned_results.matches)} returned matches.")
    
    print("\n" + "="*60)
    print("Debugging complete.")
    print(f"See results in '{output_dir}'")
    print("="*60)


if __name__ == "__main__":
    pro_players = fetch_pro_players()
    
    # This script can be configured to run in batches.
    # We have already run the first 20 players in previous steps.
    # To run the next batch, change the start and end indices.
    # Example: players_to_run = pro_players[20:30] for the third batch.
    
    players_to_run = pro_players # Run for all players in the filtered list
    
    start_date_to_run = None
    end_date_to_run = None

    total_players = len(players_to_run)
    print(f"Starting analysis for {total_players} players...")

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for player in players_to_run:
            player_id_to_run = player.get('id')
            if player_id_to_run:
                futures.append(executor.submit(run_single_player_debug, player_id_to_run, start_date_to_run, end_date_to_run))
        
        for i, future in enumerate(futures):
            future.result() # Wait for the task to complete
            print(f"--- Completed processing player {i+1}/{total_players} ---")
    # player_id_to_run = 32272
    # start_date_to_run = None
    # end_date_to_run = None
    # run_single_player_debug(player_id_to_run, start_date_to_run, end_date_to_run)
