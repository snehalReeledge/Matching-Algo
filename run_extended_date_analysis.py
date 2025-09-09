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


def get_month_range(start_date, end_date):
    """Yields start and end dates for each month in the range."""
    current_date = start_date
    while current_date <= end_date:
        month_end = current_date + relativedelta(months=1) - relativedelta(days=1)
        if month_end > end_date:
            month_end = end_date
        yield current_date, month_end
        current_date += relativedelta(months=1)

def fetch_players_with_role_player():
    """
    Fetches all players with the 'player' role, excluding those with emails containing 'reeledge.com'.
    """
    print("Fetching all players with role 'player' (excluding @reeledge.com accounts)...")
    try:
        response = requests.get(PLAYERS_API_URL, timeout=60)
        response.raise_for_status()
        all_players = response.json()

        # Filter for players with role 'player'
        player_role_players = [p for p in all_players if p.get('roles') == 'player']

        # Exclude internal accounts based on email
        filtered_players = [
            p for p in player_role_players
            if '@reeledge.com' not in p.get('email', '').lower()
        ]

        print(f"Found {len(filtered_players)} players to analyze after filtering.")
        return filtered_players
    except requests.exceptions.RequestException as e:
        print(f"Fatal: Could not fetch players from API. Error: {e}")
        return []

def analyze_single_player(player, start_date_str, end_date_str, bank_start_date_str, bank_end_date_str, checkbook_start_date_str, checkbook_end_date_str):
    """
    Analyzes transactions for a single player by fetching their specific platform and bank transactions.
    """
    player_id = player.get('id')
    player_name = player.get('name', 'Unknown Player')
    print(f"Processing player {player_id} ({player_name})...")

    try:
        # Fetch platform transactions specifically for this player
        pt_response = requests.get(
            PLATFORM_TRANSACTIONS_API_URL,
            params={'user_id': player_id, 'start_date': start_date_str, 'end_date': end_date_str},
            timeout=120
        )
        pt_response.raise_for_status()
        player_platform_transactions = pt_response.json()

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
            "received_matcher": received_matcher,
            "returned_matcher": returned_matcher,
            "received_results": received_results,
            "returned_results": returned_results,
            "platform_transactions": player_platform_transactions
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
    
    # Use default date range if start_date_str or end_date_str is None
    if start_date_str is None:
        start_date_str = "1970-01-01"  # Default start date
    if end_date_str is None:
        end_date_str = datetime.now().strftime(date_format)  # Default end date is today
    
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

    pro_players = fetch_players_with_role_player()
    if not pro_players: return

    if num_players > 0:
        players_to_analyze = pro_players[:num_players]
        print(f"\nStarting analysis for the first {len(players_to_analyze)} 'Pro' players...")
    else:
        players_to_analyze = pro_players
        print(f"\nStarting analysis for all {len(players_to_analyze)} 'Pro' players...")

    all_results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_player = {
            executor.submit(analyze_single_player, player, start_date_str, end_date_str, bank_start_date_str, bank_end_date_str, checkbook_start_date_str, checkbook_end_date_str): player
            for player in players_to_analyze
        }
        for future in as_completed(future_to_player):
            player_result = future.result()
            if player_result:
                all_results.append(player_result)

    all_results.sort(key=lambda x: x.get('player_id', 0))

    # --- Generate new detailed reports ---
    
    generate_batch_reports(all_results, start_date_str, end_date_str)
    # --- End new report generation ---

    # Generate the original summary report for compatibility
    summary_results = []
    for result in all_results:
        # If a player failed, record the error and skip them
        if "error" in result:
            summary_results.append({
                "player_id": result["player_id"],
                "player_name": result["player_name"],
                "error": result["error"]
            })
            continue

        summary_results.append({
            "player_id": result["player_id"],
            "player_name": result["player_name"],
            "platform_transactions_count": len(result["platform_transactions"]),
            "bank_transactions_count": len(result["received_matcher"].bank_transactions),
            "received_matches": len(result["received_results"].matches),
            "returned_matches": len(result["returned_results"].matches),
            "unmatched_received_count": len(result["received_results"].unmatched_platform_transactions),
            "unmatched_returned_count": len(result["returned_results"].unmatched_platform_transactions),
        })
        
    output_dir = "debug"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    output_filename = os.path.join(output_dir, f"extended_date_analysis_{num_players}_players_no_internal_{start_date_str}_to_{end_date_str}.json")
    with open(output_filename, 'w') as f:
        json.dump(summary_results, f, indent=4)
        
    print(f"\nBatch analysis complete. Results saved to '{output_filename}'")


def generate_batch_reports(all_results, start_date_str, end_date_str):
    """Generates the detailed unmatched reasons and matched pairs reports."""
    print("\nGenerating detailed batch reports...")
    
    all_unmatched_reasons = []
    all_unmatched_returned_reasons = []
    all_matched_pairs = {}
    all_metrics_summary = []

    # Helper function to investigate unmatched (adapted from single player script)
    def investigate_unmatched_received(player_id, matcher, match_results):
        findings = []
        unmatched_betting_pts = [pt for pt in match_results.unmatched_platform_transactions if 'betting bank account' in pt.get('to', {}).get('Account_Type', '').lower()]
        for unmatched_pt in unmatched_betting_pts:
            reason = "Unknown reason."
            potential_bank_matches = []
            for bt in matcher._potential_bank_transactions:
                if matcher._is_amount_match(float(unmatched_pt.get('Amount', 0)), float(bt.get('amount', 0))) and matcher._is_date_match(datetime.fromisoformat(unmatched_pt.get('Date', '').replace('Z', '+00:00')), datetime.fromisoformat(bt.get('date', '').replace('Z', '+00:00'))) and matcher._is_bank_account_match(unmatched_pt, bt):
                    potential_bank_matches.append(bt)
            if not potential_bank_matches:
                reason = "No suitable Bank Transaction found (failed amount, date, or account ID check)."
            else:
                found_match_with_used_cp = False
                for pot_bt in potential_bank_matches:
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
            findings.append({"player_id": player_id, "platform_transaction_id": unmatched_pt.get('id'), "reason_for_no_match": reason})
        return findings

    # Helper function to format matches (adapted from single player script)
    def format_matches(matches, match_type):
        formatted = []
        for match in matches:
            data = {"platform_transaction_id": match.platform_transaction.get('id'), "bank_transaction_id": match.bank_transaction.get('id')}
            if match_type == 'received' and hasattr(match, 'checkbook_payment') and match.checkbook_payment:
                data["checkbook_payment_id"] = match.checkbook_payment.get('id')
            formatted.append(data)
        return formatted

    # Helper function to investigate unmatched returned transactions
    def investigate_unmatched_returned(player_id, matcher, match_results):
        findings = []
        unmatched_betting_pts = [pt for pt in match_results.unmatched_platform_transactions if 'betting' in pt.get('from', {}).get('Account_Type', '').lower()]
        
        # Get a set of bank transaction IDs that were successfully matched
        matched_bt_ids = {match.bank_transaction.get('id') for match in match_results.matches}

        for unmatched_pt in unmatched_betting_pts:
            reason = "Unknown reason."
            potential_bank_matches = []
            
            # Find any bank transaction that *could* have been a match
            for bt in matcher._potential_bank_transactions:
                if matcher._is_amount_match(float(unmatched_pt.get('Amount', 0)), float(bt.get('amount', 0))) and \
                   matcher._is_date_match(datetime.fromisoformat(unmatched_pt.get('Date', '').replace('Z', '+00:00')), datetime.fromisoformat(bt.get('date', '').replace('Z', '+00:00'))) and \
                   matcher._is_bank_account_match(unmatched_pt, bt):
                    potential_bank_matches.append(bt)

            if not potential_bank_matches:
                reason = "No suitable Bank Transaction found (failed amount, date, or account ID check)."
            else:
                # A potential match was found, so it must have been used by another transaction.
                # The returned matcher is greedy, so we just need to see if the potential BT was matched at all.
                stolen = False
                for pot_bt in potential_bank_matches:
                    if pot_bt.get('id') in matched_bt_ids:
                        reason = f"A suitable Bank Transaction (ID: {pot_bt.get('id')}) was found, but it was already used by another Platform Transaction."
                        stolen = True
                        break
                if not stolen:
                    reason = "A suitable Bank Transaction was found, but it was not matched for an unknown reason (logic error)."

            findings.append({"player_id": player_id, "platform_transaction_id": unmatched_pt.get('id'), "reason_for_no_match": reason})
        return findings

    for result in all_results:
        # Skip players that had an error during data fetching/processing
        if "error" in result:
            player_id = result.get('player_id', 'Unknown')
            player_name = result.get('player_name', 'Unknown')
            all_metrics_summary.append(f"Player {player_id} ({player_name}): SKIPPED DUE TO API ERROR")
            continue

        player_id = result['player_id']
        
        # 1. Get unmatched reasons
        unmatched_received = investigate_unmatched_received(player_id, result['received_matcher'], result['received_results'])
        all_unmatched_reasons.extend(unmatched_received)
        
        unmatched_returned = investigate_unmatched_returned(player_id, result['returned_matcher'], result['returned_results'])
        all_unmatched_returned_reasons.extend(unmatched_returned)

        # 2. Get matched pairs
        all_matched_pairs[player_id] = {
            "received_matches": format_matches(result['received_results'].matches, 'received'),
            "returned_matches": format_matches(result['returned_results'].matches, 'returned')
        }

        # 3. Get metrics
        total_betting_received = sum(1 for pt in result['platform_transactions'] if pt.get('Transaction_Type') == 'received' and 'betting' in pt.get('to', {}).get('Account_Type', '').lower())
        matched_betting_received = sum(1 for match in result['received_results'].matches if 'betting' in match.platform_transaction.get('to', {}).get('Account_Type', '').lower())
        received_rate = (matched_betting_received / total_betting_received * 100) if total_betting_received > 0 else 100
        
        total_betting_returned = sum(1 for pt in result['platform_transactions'] if pt.get('Transaction_Type') == 'returned' and 'betting' in pt.get('from', {}).get('Account_Type', '').lower())
        matched_betting_returned = sum(1 for match in result['returned_results'].matches if 'betting' in match.platform_transaction.get('from', {}).get('Account_Type', '').lower())
        returned_rate = (matched_betting_returned / total_betting_returned * 100) if total_betting_returned > 0 else 100

        all_metrics_summary.append(f"Player {player_id}: Received Betting Bank Match Rate: {matched_betting_received}/{total_betting_received} ({received_rate:.2f}%) | Returned Betting Bank Match Rate: {matched_betting_returned}/{total_betting_returned} ({returned_rate:.2f}%)")

    output_dir = "debug"
    # Save unmatched received reasons
    unmatched_received_filename = os.path.join(output_dir, f"unmatched_betting_bank_reasons_{start_date_str}_to_{end_date_str}.json")
    with open(unmatched_received_filename, 'w') as f:
        json.dump(all_unmatched_reasons, f, indent=4)
    print(f" -> Saved detailed unmatched received reasons to '{unmatched_received_filename}'")

    # Save unmatched returned reasons
    unmatched_returned_filename = os.path.join(output_dir, f"unmatched_returned_betting_bank_reasons_{start_date_str}_to_{end_date_str}.json")
    with open(unmatched_returned_filename, 'w') as f:
        json.dump(all_unmatched_returned_reasons, f, indent=4)
    print(f" -> Saved detailed unmatched returned reasons to '{unmatched_returned_filename}'")

    # Save matched pairs
    matched_filename = os.path.join(output_dir, f"matched_pairs_{start_date_str}_to_{end_date_str}.json")
    with open(matched_filename, 'w') as f:
        json.dump(all_matched_pairs, f, indent=4)
    print(f" -> Saved matched pairs to '{matched_filename}'")

    # Save summary metrics
    summary_filename = os.path.join(output_dir, f"matching_summary_{start_date_str}_to_{end_date_str}.txt")
    with open(summary_filename, 'w') as f:
        f.write("\n".join(all_metrics_summary))
    print(f" -> Saved summary metrics to '{summary_filename}'")


if __name__ == "__main__":
    NUM_PLAYERS_TO_ANALYZE = 0 # Set to 0 or a negative number to run for all players
    START_DATE = "2024-01-01"
    END_DATE = "2025-07-31"
    run_extended_date_analysis(NUM_PLAYERS_TO_ANALYZE, START_DATE, END_DATE)
