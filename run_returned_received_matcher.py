import json
import requests
import os
import time
import argparse
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

from update_bank_transaction import update_bank_transaction
from update_platform_transaction import update_platform_transaction

# Import from existing modules
from config import (
    PLATFORM_TRANSACTIONS_API_URL,
    BANK_TRANSACTIONS_API_URL,
    RETURNED_KEYWORDS,
    PLAYERS_API_URL
)
from received_transaction_matcher import SimpleReceivedTransactionMatcher
from returned_transaction_matcher import SimpleTransactionMatcher as ReturnedTransactionMatcher

def fetch_players_by_stage(stage):
    """Fetches all players for a given stage, excluding internal accounts."""
    print(f"Fetching '{stage}' players (excluding @reeledge.com)...")
    try:
        response = requests.get(PLAYERS_API_URL, timeout=60)
        response.raise_for_status()
        
        all_players = response.json()
        stage_players = [p for p in all_players if p.get('player_stage') == stage]
        filtered_players = [p for p in stage_players if '@reeledge.com' not in p.get('email', '').lower()]
        
        print(f"Found {len(filtered_players)} '{stage}' players to analyze.")
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

        bank_response = requests.get(
            BANK_TRANSACTIONS_API_URL, 
            params={'player_id': player_id, '_': int(time.time())}, 
            timeout=60
        )
        bank_response.raise_for_status()
        bank_transactions = bank_response.json().get('bankTransactions', [])

        # Sort bank transactions by date in ascending order
        bank_transactions.sort(key=lambda x: x.get('date', ''))

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
    timestamp = datetime.now().strftime("%Y_%m_%d")
    # output_dir = f"@debug_transfers_{timestamp}/player_{player_id}"
    # if not os.path.exists(output_dir):
    #     os.makedirs(output_dir)
    
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

    # print(f"Output will be saved in: {output_dir}")
    print("="*60)

    # 2. Run Analysis
    print("\n[Step 1/7] Running transaction matching analysis...")
    received_matcher, returned_matcher, platform_transactions, received_results, returned_results = analyze_player_transactions(player_id, start_date_str, end_date_str, date_ranges)
    if not received_matcher:
        return 0, 0
    print(" -> Analysis complete.")

    # 3. Dump raw data for inspection
    print("\n[Step 2/7] Dumping raw data for inspection...")
    # output_pt_dump_file = os.path.join(output_dir, "platform_transactions_dump.json")
    # with open(output_pt_dump_file, 'w') as f:
    #     json.dump(platform_transactions, f, indent=4)
        
    # output_cp_dump_file = os.path.join(output_dir, "checkbook_payments_dump.json")
    # with open(output_cp_dump_file, 'w') as f:
    #     json.dump({
    #         "all_checkbook_payments": received_matcher.checkbook_payments,
    #         "valid_checkbook_payments_for_received": received_matcher._valid_checkbook_payments
    #     }, f, indent=4)
        
    # all_bank_transactions = getattr(returned_matcher, 'bank_transactions', [])
    # output_bt_dump_file = os.path.join(output_dir, "bank_transactions_dump.json")
    # with open(output_bt_dump_file, 'w') as f:
    #     json.dump(all_bank_transactions, f, indent=4)
    print(" -> Raw data dump skipped.")

    # 4. Run Investigation
    print("\n[Step 3/7] Investigating unmatched betting bank transactions...")
    unmatched_reasons = investigate_unmatched_received(player_id, received_matcher, received_results)
    
    # output_investigation_file = os.path.join(output_dir, "unmatched_betting_bank_reasons.json")
    # with open(output_investigation_file, 'w') as f:
    #     json.dump(unmatched_reasons, f, indent=4)
    print(f" -> Investigation complete. Found {len(unmatched_reasons)} unmatched.")

    # 5. Calculate Metrics
    print("\n[Step 4/7] Calculating betting bank matching percentages...")
    metrics = calculate_metrics(received_matcher, returned_matcher, platform_transactions, received_results, returned_results)
    
    # output_metrics_file = os.path.join(output_dir, "matching_summary.txt")
    # with open(output_metrics_file, 'w') as f:
    #     f.write("--- Betting Bank Matching Metrics ---\n")
    #     f.write(f"Received: {metrics['received']['matched']}/{metrics['received']['total']} ({metrics['received']['rate']})\n")
    #     f.write(f"Returned: {metrics['returned']['matched']}/{metrics['returned']['total']} ({metrics['returned']['rate']})\n")
    print(" -> Metrics calculation complete.")
    
    # 6. Update Bank Transaction Links
    print("\n[Step 5/7] Updating bank transaction links for matched items...")
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

    # 7. Update Platform Transaction Dates
    print("\n[Step 6/7] Syncing dates for matched transactions with different dates...")
    dates_synced_count = 0
    all_matches = received_results.matches + returned_results.matches
    for match in all_matches:
        platform_date_str = match.platform_transaction.get('Date', '').split('T')[0]
        bank_date_str = match.bank_transaction.get('date', '').split('T')[0]

        if platform_date_str and bank_date_str and platform_date_str != bank_date_str:
            platform_id = match.platform_transaction.get('id')
            bank_date = match.bank_transaction.get('date')
            if platform_id and bank_date:
                formatted_date = bank_date.split('T')[0]
                
                # Prepare the note
                old_notes = match.platform_transaction.get('Notes', '') or ''
                note_to_add = f"System Matched: Date updated from {platform_date_str} to {bank_date_str} on {datetime.now().strftime('%Y-%m-%d')}."
                new_notes = f"{old_notes}\n{note_to_add}".strip()
                
                update_platform_transaction(platform_id, {"Date": formatted_date, "Notes": new_notes})
                dates_synced_count += 1
    print(f" -> Date sync requests sent for {dates_synced_count} transactions.")


    # 8. Save Matched and Unmatched Pairs
    print("\n[Step 7/7] Saving all matched and unmatched transaction pairs...")

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
    
    # output_matches_file = os.path.join(output_dir, "matched_pairs.json")
    # with open(output_matches_file, 'w') as f:
    #     json.dump(all_matches, f, indent=4)

    unmatched_data = {
        "unmatched_platform_received": received_results.unmatched_platform_transactions,
        "unmatched_bank_for_received": received_results.unmatched_bank_transactions,
        "unmatched_checkbook_payments": received_results.unmatched_checkbook_payments,
        "unmatched_platform_returned": returned_results.unmatched_platform_transactions,
        "unmatched_bank_for_returned": returned_results.unmatched_bank_transactions
    }
    # output_unmatched_file = os.path.join(output_dir, "unmatched_transactions.json")
    # with open(output_unmatched_file, 'w') as f:
    #     json.dump(unmatched_data, f, indent=4)
        
    print(f" -> Found {len(received_results.matches)} received and {len(returned_results.matches)} returned matches to link.")
    
    print("\n" + "="*60)
    print("Analysis complete.")
    # print(f"See results in '{output_dir}'")
    print("="*60)
    
    return updated_count, dates_synced_count


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run matching for players in one or more specific stages or for a single player.")
    parser.add_argument("--player_id", type=str, default=None, help="A specific player ID to run for.")
    parser.add_argument("player_stages", nargs='*', type=str, help="One or more player stages to process (e.g., 'Batch 1' 'Batch 2').")
    parser.add_argument("--start_date", type=str, default=None, help="Start date in YYYY-MM-DD format.")
    parser.add_argument("--end_date", type=str, default=None, help="End date in YYYY-MM-DD format.")
    args = parser.parse_args()

    if not args.player_id and not args.player_stages:
        parser.error("You must provide either --player_id or one or more player_stages.")
    if args.player_id and args.player_stages:
        parser.error("Please provide either --player_id or player_stages, not both.")

    players_to_run = []
    if args.player_id:
        players_to_run = [{'id': args.player_id}]
    else:
        for stage in args.player_stages:
            players_to_run.extend(fetch_players_by_stage(stage))
    
    start_date_to_run = args.start_date
    end_date_to_run = args.end_date

    if players_to_run:
        total_players = len(players_to_run)
        print(f"Starting analysis for {total_players} players...")

        total_linked = 0
        total_synced = 0
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            for player in players_to_run:
                player_id_to_run = player.get('id')
                if player_id_to_run:
                    futures.append(executor.submit(run_single_player_debug, player_id_to_run, start_date_to_run, end_date_to_run))
            
            for i, future in enumerate(futures):
                linked_count, synced_count = future.result() # Wait for the task to complete
                total_linked += linked_count
                total_synced += synced_count
                print(f"--- Completed processing player {i+1}/{total_players} ---")
        
        print("\n" + "="*60)
        print("--- BATCH SUMMARY ---")
        print(f"Total bank transactions linked: {total_linked}")
        print(f"Total platform transaction dates synced: {total_synced}")
        print("="*60)
