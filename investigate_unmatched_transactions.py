import json
import requests
import os
import time
from datetime import datetime, timedelta

# Import from existing modules
from config import (
    PLAYERS_API_URL,
    PLATFORM_TRANSACTIONS_API_URL,
    BANK_TRANSACTIONS_API_URL,
    RETURNED_KEYWORDS
)
from received_transaction_matcher import SimpleReceivedTransactionMatcher
from returned_transaction_matcher import SimpleTransactionMatcher as ReturnedTransactionMatcher

def get_players_to_investigate():
    """
    Fetches players with the 'player' role, reads the latest analysis file,
    and returns the intersection of players who need investigation and have the correct role.
    """
    # Step 1: Fetch all players and filter for roles == 'player'
    print("Fetching all players to filter by role...")
    valid_player_ids = set()
    try:
        response = requests.get(PLAYERS_API_URL, timeout=60)
        response.raise_for_status()
        all_players = response.json()
        
        player_role_players = [p for p in all_players if p.get('roles') == 'player']
        valid_player_ids = {p['id'] for p in player_role_players}
        print(f"Found {len(valid_player_ids)} players with the 'player' role.")

    except requests.exceptions.RequestException as e:
        print(f"Fatal: Could not fetch players from API. Error: {e}")
        return []

    # Step 2: Read the analysis file to find players with unmatched transactions
    analysis_file = 'debug/extended_date_analysis_0_players_no_internal_2025-05-01_to_2025-07-31.json'
    unmatched_player_ids = set()
    try:
        with open(analysis_file, 'r') as f:
            results = json.load(f)
        
        for p in results:
            if p.get('unmatched_received_count', 0) > 0 or p.get('unmatched_returned_count', 0) > 0:
                unmatched_player_ids.add(p['player_id'])
        print(f"Found {len(unmatched_player_ids)} players with unmatched transactions in the analysis file.")

    except FileNotFoundError:
        print(f"Error: The analysis file '{analysis_file}' was not found.")
        return []
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from '{analysis_file}'.")
        return []

    # Step 3: Find the intersection
    final_player_ids = sorted(list(unmatched_player_ids.intersection(valid_player_ids)))
    print(f"Found {len(final_player_ids)} players to investigate (unmatched transactions and 'player' role).")
    return final_player_ids

def investigate_unmatched_received(player_id, start_date_str, end_date_str, date_ranges):
    """
    Performs a deep-dive analysis on a single player to determine why
    'received' transactions to a betting bank account were not matched.
    """
    findings = []
    try:
        pt_response = requests.get(PLATFORM_TRANSACTIONS_API_URL, params={'user_id': player_id, 'start_date': start_date_str, 'end_date': end_date_str}, timeout=120)
        pt_response.raise_for_status()
        player_platform_transactions = pt_response.json()

        bank_response = requests.get(BANK_TRANSACTIONS_API_URL, params={'player_id': player_id, 'start_date': date_ranges['bank_start'], 'end_date': date_ranges['bank_end']}, timeout=60)
        bank_response.raise_for_status()
        player_bank_transactions = bank_response.json().get('bankTransactions', [])

        matcher = SimpleReceivedTransactionMatcher(player_platform_transactions, player_bank_transactions, player_id, date_ranges['checkbook_start'], date_ranges['checkbook_end'])
        results = matcher.match_received_transactions()
        
        # Filter for only unmatched transactions to a betting bank account
        unmatched_betting_pts = [
            pt for pt in results.unmatched_platform_transactions
            if 'betting bank account' in pt.get('to', {}).get('Account_Type', '').lower()
        ]

        if not unmatched_betting_pts:
            return []

        print(f"  -> Investigating {len(unmatched_betting_pts)} unmatched RECEIVED (Betting Bank) for player {player_id}...")

        for unmatched_pt in unmatched_betting_pts:
            potential_bank_matches = []
            for bt in matcher._potential_bank_transactions:
                if matcher._is_amount_match(float(unmatched_pt.get('Amount', 0)), float(bt.get('amount', 0))) and \
                   matcher._is_date_match(datetime.fromisoformat(unmatched_pt.get('Date', '').replace('Z', '+00:00')), datetime.fromisoformat(bt.get('date', '').replace('Z', '+00:00'))) and \
                   matcher._is_bank_account_match(unmatched_pt, bt):
                    potential_bank_matches.append(bt)
            
            reason = ""
            if not potential_bank_matches:
                reason = "No suitable Bank Transaction found. No bank transaction met all three criteria: Amount, Date (within ±5 days), and Bank Account ID."
            else:
                final_match_found = False
                for pot_bt in potential_bank_matches:
                    checkbook_payment = matcher._find_matching_checkbook_payment(pot_bt, unmatched_pt)
                    if checkbook_payment:
                        reason = f"A valid checkbook payment (ID: {checkbook_payment.get('id')}) was found for Bank Transaction ID {pot_bt.get('id')}, but this pair was likely used for a different, higher-priority match."
                        final_match_found = True
                        break
                if not final_match_found:
                    reason = "A potential Bank Transaction was found, but no valid Checkbook Payment could be matched to it. The checkbook payment must match amount, recipient, description, direction, and have a date within ±3 days of the bank transaction."

            findings.append({
                "player_id": player_id,
                "transaction_type": "received",
                "platform_transaction_id": unmatched_pt.get('id'),
                "platform_transaction_amount": unmatched_pt.get('Amount'),
                "platform_transaction_date": unmatched_pt.get('Date'),
                "from_account_type": unmatched_pt.get('from', {}).get('Account_Type'),
                "to_account_type": unmatched_pt.get('to', {}).get('Account_Type'),
                "reason_for_no_match": reason
            })
    except requests.exceptions.RequestException as e:
        print(f"  -> API Error for player {player_id} (received): {e}")
    return findings

def investigate_unmatched_returned(player_id, start_date_str, end_date_str, date_ranges):
    """
    Performs a deep-dive analysis on a single player to determine why
    'returned' transactions from a betting bank account were not matched.
    """
    findings = []
    try:
        pt_response = requests.get(PLATFORM_TRANSACTIONS_API_URL, params={'user_id': player_id, 'start_date': start_date_str, 'end_date': end_date_str}, timeout=120)
        pt_response.raise_for_status()
        player_platform_transactions = pt_response.json()

        bank_response = requests.get(BANK_TRANSACTIONS_API_URL, params={'player_id': player_id, 'start_date': date_ranges['bank_start'], 'end_date': date_ranges['bank_end']}, timeout=60)
        bank_response.raise_for_status()
        player_bank_transactions = bank_response.json().get('bankTransactions', [])

        matcher = ReturnedTransactionMatcher(player_platform_transactions, player_bank_transactions, RETURNED_KEYWORDS)
        results = matcher.match_returned_transactions()

        # Filter for only unmatched transactions from a betting bank account
        unmatched_betting_pts = [
            pt for pt in results.unmatched_platform_transactions
            if 'betting bank account' in pt.get('from', {}).get('Account_Type', '').lower()
        ]

        if not unmatched_betting_pts:
            return []

        print(f"  -> Investigating {len(unmatched_betting_pts)} unmatched RETURNED (Betting Bank) for player {player_id}...")

        for unmatched_pt in unmatched_betting_pts:
            reason = ""
            potential_bank_transactions = matcher._potential_bank_transactions
            
            if not potential_bank_transactions:
                reason = "No potential bank transactions were found. A bank transaction must have a POSITIVE amount and a KEYWORD in the description to be considered a potential match."
            else:
                found_a_perfect_match = False
                reasons_for_mismatch = []
                for bt in potential_bank_transactions:
                    amount_match = matcher._is_amount_match(float(unmatched_pt.get('Amount', 0)), float(bt.get('amount', 0)))
                    date_match = matcher._is_date_match(datetime.fromisoformat(unmatched_pt.get('Date', '').replace('Z', '+00:00')), datetime.fromisoformat(bt.get('date', '').replace('Z', '+00:00')))
                    account_match = matcher._is_bank_account_match(unmatched_pt, bt)
                    
                    if amount_match and date_match and account_match:
                        found_a_perfect_match = True
                        break
                    else:
                        mismatches = []
                        if not amount_match: mismatches.append("amount")
                        if not date_match: mismatches.append("date")
                        if not account_match: mismatches.append("account ID")
                        reasons_for_mismatch.append(f"BT ID {bt.get('id')}: failed on {', '.join(mismatches)}")
                
                if found_a_perfect_match:
                    reason = "A perfectly matching bank transaction was found, but it was likely used for another, higher-priority platform transaction."
                else:
                    reason = "No bank transactions met all three criteria. Mismatch details: " + "; ".join(reasons_for_mismatch)

            findings.append({
                "player_id": player_id,
                "transaction_type": "returned",
                "platform_transaction_id": unmatched_pt.get('id'),
                "platform_transaction_amount": unmatched_pt.get('Amount'),
                "platform_transaction_date": unmatched_pt.get('Date'),
                "from_account_type": unmatched_pt.get('from', {}).get('Account_Type'),
                "to_account_type": unmatched_pt.get('to', {}).get('Account_Type'),
                "reason_for_no_match": reason
            })
    except requests.exceptions.RequestException as e:
        print(f"  -> API Error for player {player_id} (returned): {e}")
    return findings

def run_investigation(start_date_str, end_date_str):
    """Orchestrates the investigation for all players with unmatched transactions."""
    
    player_ids = get_players_to_investigate()
    if not player_ids:
        return

    date_format = "%Y-%m-%d"
    start_date = datetime.strptime(start_date_str, date_format)
    end_date = datetime.strptime(end_date_str, date_format)
    date_ranges = {
        'bank_start': (start_date - timedelta(days=5)).strftime(date_format),
        'bank_end': (end_date + timedelta(days=5)).strftime(date_format),
        'checkbook_start': (start_date - timedelta(days=7)).strftime(date_format),
        'checkbook_end': (end_date + timedelta(days=7)).strftime(date_format)
    }

    print("\n" + "="*60)
    print("Starting Deep-Dive Investigation of Unmatched Transactions")
    print("="*60 + "\n")

    all_findings = []
    for player_id in player_ids:
        print(f"Processing player {player_id}...")
        
        received_findings = investigate_unmatched_received(player_id, start_date_str, end_date_str, date_ranges)
        all_findings.extend(received_findings)
        
        returned_findings = investigate_unmatched_returned(player_id, start_date_str, end_date_str, date_ranges)
        all_findings.extend(returned_findings)
        
        time.sleep(1) # Pause for 1 second to be respectful to the API

    # Save the results to a file
    output_dir = "debug"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    output_filename = os.path.join(output_dir, f"unmatched_betting_bank_reasons_{start_date_str}_to_{end_date_str}.json")
    with open(output_filename, 'w') as f:
        json.dump(all_findings, f, indent=4)
        
    print(f"\nInvestigation complete. Results saved to '{output_filename}'")

if __name__ == "__main__":
    START_DATE = "2025-05-01"
    END_DATE = "2025-07-31"
    run_investigation(START_DATE, END_DATE)
