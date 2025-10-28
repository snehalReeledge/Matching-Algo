from transfer_matcher import TransferTransactionMatcher, get_all_platform_transactions, get_all_bank_transactions
from config import TRANSFER_ACCOUNT_ID
from update_bank_transaction import update_bank_transaction
from update_platform_transaction import update_platform_transaction
from create_platform_transaction import create_platform_transaction
import json
import os
from datetime import datetime
import time
import argparse
from concurrent.futures import ThreadPoolExecutor
from config import PLAYERS_API_URL
import requests

class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle datetime objects."""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super(DateTimeEncoder, self).default(obj)

def get_players_by_stage(stage: str) -> list[dict]:
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

def run_matcher_for_player(player_id, delay_seconds=0, no_debug_files=False):
    """
    Runs the full matching process and saves a detailed investigation report with proposed actions.
    """
    if delay_seconds > 0:
        time.sleep(delay_seconds)

    print(f"--- Starting Matcher for Player ID: {player_id} ---")
    
    # --- 1. Fetch data ---
    platform_transactions = get_all_platform_transactions(player_id)
    
    if not platform_transactions:
        print("Could not fetch platform transactions. Exiting.")
        return
    
    # Check for relevant transfer transactions before proceeding
    relevant_transfers = [
        pt for pt in platform_transactions
        if (pt.get('Transaction_Type', '').lower() == 'transfer' and
            pt.get('from', {}).get('Account_Type', '').lower() == 'betting bank account' and
            pt.get('to', {}).get('Account_Type', '').lower() == 'betting paypal account')
    ]

    if not relevant_transfers:
        print(f"  -> No relevant transfer transactions found for Player ID: {player_id}. Skipping.")
        return None # Return None to indicate no processing was done
        
    bank_transactions = get_all_bank_transactions(player_id)
    
    if not bank_transactions:
        print("Could not fetch bank transactions. Exiting.")
        return None

    # --- 2. Create the output directory and save raw data for debugging ---
    output_dir = None
    if not no_debug_files:
        today_str = datetime.now().strftime('%Y_%m_%d')
        base_debug_dir = f"@debug_transfers_{today_str}"
        output_dir = os.path.join(base_debug_dir, f"player_{player_id}_investigation")
        os.makedirs(output_dir, exist_ok=True)
        
        # Save raw platform transactions BEFORE matching
        with open(os.path.join(output_dir, f"platform_transactions_{player_id}_raw.json"), 'w') as f:
            json.dump(platform_transactions, f, indent=2)

    # --- 3. Run the matcher ---
    matcher = TransferTransactionMatcher(platform_transactions, bank_transactions)
    results = matcher.match_transactions()
    
    # --- 4. Prepare and execute actions based on matches ---
    print(f"  -> Found {len(results.simple_matches)} simple matches to execute.")
    
    # Simple Matches
    for match in results.simple_matches:
        pt = match.platform_transaction
        bt = match.bank_transaction
        
        # Link bank transaction
        print(f"\nAction: LINK_SIMPLE for PT ID {pt.get('id')} and BT ID {bt.get('transaction_id')}")
        update_bank_transaction(bt.get('transaction_id'), pt.get('id'))

        # Sync platform transaction date if it's different from the bank transaction date
        platform_date_str = pt.get('Date', '').split('T')[0]
        bank_date_str = bt.get('date', '').split('T')[0]

        if platform_date_str and bank_date_str and platform_date_str != bank_date_str:
            print(f"Action: SYNC_DATE for PT ID {pt.get('id')}. Updating date from {platform_date_str} to {bank_date_str}")
            
            # Prepare the note
            old_notes = pt.get('Notes', '') or ''
            note_to_add = f"System Matched: Date updated from {platform_date_str} to {bank_date_str} on {datetime.now().strftime('%Y-%m-%d')}."
            new_notes = f"{old_notes}\n{note_to_add}".strip()

            update_payload = {
                "Date": bank_date_str,
                "Notes": new_notes
            }
            update_platform_transaction(pt.get('id'), update_payload)
        
    # --- 5. Write output files if not disabled ---
    if not no_debug_files and output_dir:
        try:
            # proposed_actions.json
            proposed_actions = []
            for match in results.simple_matches:
                proposed_actions.append({
                    "action": "LINK_SIMPLE",
                    "platform_transaction_id": match.platform_transaction.get('id'),
                    "bank_transaction_id": match.bank_transaction.get('id')
                })
            with open(os.path.join(output_dir, "proposed_actions.json"), 'w') as f:
                json.dump(proposed_actions, f, indent=4)
            
            # summary.txt
            summary_path = os.path.join(output_dir, "matching_summary.txt")
            with open(summary_path, 'w') as f:
                f.write(f"--- Matching Summary for Player ID: {player_id} ---\n")
                f.write(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                f.write(f"Total Platform Transactions Fetched: {len(platform_transactions)}\n")
                f.write(f"Total Bank Transactions Fetched: {len(bank_transactions)}\n\n")
                f.write(f"Simple Matches Found: {len(results.simple_matches)}\n")
                f.write(f"Unmatched Platform Transfers: {len(results.unmatched_platform_transactions)}\n")
                
            # unmatched_analysis.json
            unmatched_output = []
            for unmatched in results.unmatched_platform_transactions:
                unmatched_output.append({
                    "platform_transaction": unmatched.platform_transaction,
                    "reason": unmatched.reason
                })
            
            analysis_path = os.path.join(output_dir, "unmatched_analysis.json")
            with open(analysis_path, 'w') as f:
                json.dump(unmatched_output, f, indent=4, cls=DateTimeEncoder)
            
            print(f"  -> Successfully created investigation report in: {output_dir}")

        except IOError as e:
            print(f"Error writing output files: {e}")

    print(f"--- Matcher Finished for Player ID: {player_id} ---")
    
    # --- 6. Return a summary for consolidated reporting ---
    return {
        "player_id": player_id,
        "summary": {
            "platform_transactions_fetched": len(platform_transactions),
            "bank_transactions_fetched": len(bank_transactions),
            "simple_matches_found": len(results.simple_matches),
            "unmatched_platform_transfers": len(results.unmatched_platform_transactions)
        },
        "unmatched_analysis": [
            {
                "platform_transaction": um.platform_transaction,
                "reason": um.reason
            } for um in results.unmatched_platform_transactions
        ]
    }

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run matching for players in specific stages or for a single player.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--player_stages", nargs='+', type=str, help="One or more player stages to process (e.g., 'Batch 1' 'Batch 2').")
    group.add_argument("--player_id", type=int, help="A specific player ID to process.")
    parser.add_argument("--max_workers", type=int, default=15, help="Maximum number of concurrent players to process.")
    parser.add_argument("--no-debug-files", action="store_true", help="Disable the creation of individual player debug files and reports.")
    args = parser.parse_args()

    player_ids_to_process = []
    if args.player_id:
        player_ids_to_process.append(args.player_id)
    elif args.player_stages:
        all_players = []
        for stage in args.player_stages:
            all_players.extend(get_players_by_stage(stage))
        player_ids_to_process = [p['id'] for p in all_players]
    
    # --- Batch processing settings ---
    # Number of players to process concurrently.
    MAX_WORKERS = args.max_workers
    # Delay in seconds between starting each player's task to avoid overwhelming the API.
    DELAY_BETWEEN_TASKS = 2

    if player_ids_to_process:
        total_players = len(player_ids_to_process)
        print(f"Starting batch processing for {total_players} players with {MAX_WORKERS} concurrent workers.")

        batch_results = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Assign a delay to each task to stagger the API calls
            futures = {executor.submit(run_matcher_for_player, player_id, i * DELAY_BETWEEN_TASKS, args.no_debug_files): player_id for i, player_id in enumerate(player_ids_to_process)}
            
            for i, future in enumerate(futures):
                result = future.result()  # Wait for the task to complete and handle any exceptions
                if result:
                    batch_results.append(result)
                print(f"--- Completed processing player {i + 1}/{total_players} ---")

        print("\n--- Batch processing complete. ---")

        # --- Save consolidated report ---
        if batch_results:
            today_str = datetime.now().strftime('%Y_%m_%d_%H%M%S')
            report_filename = f"consolidated_report_{today_str}.json"
            with open(report_filename, 'w') as f:
                json.dump(batch_results, f, indent=4, cls=DateTimeEncoder)
            print(f"✅ Consolidated report saved to: {report_filename}")
        else:
            print("No results to report.")

    else:
        print("No players found for the specified stage. Exiting.")
