#!/usr/bin/env python3
"""
Run comprehensive matching for a player including three-way matching
Creates detailed match files showing linked, created, and updated records
"""

import json
import os
from datetime import datetime
import time
from concurrent.futures import ThreadPoolExecutor
from paypal_to_bank_matcher import (
    PayPalToBankMatcher, 
    get_all_platform_transactions, 
    get_all_bank_transactions, 
    get_scraped_transactions,
    execute_three_way_match,
    link_bank_transaction
)

def create_output_directory(player_id: int, base_dir: str = None):
    """Create output directory for match results."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if base_dir:
        # For batch runs, create a subdirectory for the player
        output_dir = os.path.join(base_dir, f"player_{player_id}")
    else:
        # For single runs, create a standalone directory
        output_dir = f"player_{player_id}_matching_results_{timestamp}"
    
    os.makedirs(output_dir, exist_ok=True)
    return output_dir

def save_match_results(output_dir, results, player_id):
    """Save comprehensive match results to files."""
    
    # 1. Simple Matches Summary
    simple_matches_file = os.path.join(output_dir, "simple_matches.json")
    simple_matches_data = []
    for match in results.simple_matches:
        simple_matches_data.append({
            "platform_transaction_id": match.platform_transaction.get('id'),
            "platform_transaction_amount": match.platform_transaction.get('Amount'),
            "platform_transaction_date": match.platform_transaction.get('Date'),
            "from_account": match.platform_transaction.get('from', {}).get('Account_Name'),
            "to_account": match.platform_transaction.get('to', {}).get('Account_Name'),
            "bank_transaction_id": match.bank_transaction.get('id'),
            "bank_transaction_amount": match.bank_transaction.get('amount'),
            "bank_transaction_date": match.bank_transaction.get('date'),
            "bank_transaction_name": match.bank_transaction.get('name'),
            "match_date": match.match_date,
            "match_type": "simple"
        })
    
    with open(simple_matches_file, 'w') as f:
        json.dump(simple_matches_data, f, indent=2)
    
    # 2. Three-Way Matches Summary
    three_way_matches_file = os.path.join(output_dir, "three_way_matches.json")
    three_way_matches_data = []
    for match in results.three_way_matches:
        paypal_bt_details = {
            "paypal_bank_transaction_id": None,
            "paypal_bank_transaction_amount": None,
            "paypal_bank_transaction_date": None,
            "paypal_bank_transaction_name": None
        }
        if match.paypal_bank_transaction:
            paypal_bt_details = {
                "paypal_bank_transaction_id": match.paypal_bank_transaction.get('id'),
                "paypal_bank_transaction_amount": match.paypal_bank_transaction.get('amount'),
                "paypal_bank_transaction_date": match.paypal_bank_transaction.get('date'),
                "paypal_bank_transaction_name": match.paypal_bank_transaction.get('name')
            }

        three_way_matches_data.append({
            "original_platform_transaction_id": match.original_platform_transaction.get('id'),
            "original_platform_transaction_amount": match.original_platform_transaction.get('Amount'),
            "original_platform_transaction_date": match.original_platform_transaction.get('Date'),
            **paypal_bt_details,
            "scraped_transaction_id": match.scraped_transaction.get('id'),
            "scraped_transaction_gross": match.scraped_transaction.get('Gross'),
            "scraped_transaction_fee": match.scraped_transaction.get('Fee'),
            "net_amount": match.net_amount,
            "fee_amount": match.fee_amount,
            "match_date": match.match_date,
            "match_type": "three_way"
        })
    
    with open(three_way_matches_file, 'w') as f:
        json.dump(three_way_matches_data, f, indent=2)
    
    # 3. Unmatched Analysis
    unmatched_file = os.path.join(output_dir, "unmatched_analysis.json")
    unmatched_data = []
    for unmatched in results.unmatched_platform_transactions:
        unmatched_data.append({
            "platform_transaction_id": unmatched.platform_transaction.get('id'),
            "platform_transaction_amount": unmatched.platform_transaction.get('Amount'),
            "platform_transaction_date": unmatched.platform_transaction.get('Date'),
            "from_account": unmatched.platform_transaction.get('from', {}).get('Account_Name'),
            "to_account": unmatched.platform_transaction.get('to', {}).get('Account_Name'),
            "reason": unmatched.reason
        })
    
    with open(unmatched_file, 'w') as f:
        json.dump(unmatched_data, f, indent=2)
    
    # 4. Summary Report
    summary_file = os.path.join(output_dir, "matching_summary.txt")
    with open(summary_file, 'w') as f:
        f.write(f"PayPal to Bank Matching Results for Player {player_id}\n")
        f.write(f"Generated: {datetime.now().isoformat()}\n")
        f.write("=" * 60 + "\n\n")
        
        f.write(f"Total Simple Matches: {len(results.simple_matches)}\n")
        f.write(f"Total Three-Way Matches: {len(results.three_way_matches)}\n")
        f.write(f"Total Unmatched Platform Transactions: {len(results.unmatched_platform_transactions)}\n\n")
        
        if results.simple_matches:
            f.write("SIMPLE MATCHES:\n")
            f.write("-" * 20 + "\n")
            for match in results.simple_matches:
                f.write(f"PT {match.platform_transaction.get('id')} (${match.platform_transaction.get('Amount')}) ")
                f.write(f"<-> BT {match.bank_transaction.get('id')} (${match.bank_transaction.get('amount')})\n")
            f.write("\n")
        
        if results.three_way_matches:
            f.write("THREE-WAY MATCHES:\n")
            f.write("-" * 20 + "\n")
            for match in results.three_way_matches:
                f.write(f"PT {match.original_platform_transaction.get('id')} (${match.original_platform_transaction.get('Amount')}) ")
                if match.paypal_bank_transaction:
                    f.write(f"-> BT {match.paypal_bank_transaction.get('id')} (${match.paypal_bank_transaction.get('amount')}) ")
                else:
                    f.write("-> (No PayPal BT Found) ")
                f.write(f"with ${match.fee_amount} fees\n")
                f.write(f"  Scraped Transaction: ID {match.scraped_transaction.get('id')}, ")
                f.write(f"Gross=${abs(float(match.scraped_transaction.get('Gross', 0)))}, ")
                f.write(f"Fee=${abs(float(match.scraped_transaction.get('Fee', 0)))}\n")
            f.write("\n")
        
        if results.unmatched_platform_transactions:
            f.write("UNMATCHED TRANSACTIONS:\n")
            f.write("-" * 25 + "\n")
            for unmatched in results.unmatched_platform_transactions:
                f.write(f"PT {unmatched.platform_transaction.get('id')} (${unmatched.platform_transaction.get('Amount')}) ")
                f.write(f"on {unmatched.platform_transaction.get('Date')}\n")
                f.write(f"Reason: {unmatched.reason}\n\n")
    
    return {
        'simple_matches_file': simple_matches_file,
        'three_way_matches_file': three_way_matches_file,
        'unmatched_file': unmatched_file,
        'summary_file': summary_file
    }

def execute_simple_matches(simple_matches):
    """Executes simple matches by linking the transactions."""
    if not simple_matches:
        print("\n--- No Simple Matches to Execute ---")
        return
    
    print(f"\n--- Executing {len(simple_matches)} Simple Matches ---")
    success_count = 0
    for match in simple_matches:
        pt_id = match.platform_transaction.get('id')
        bt_transaction_id = match.bank_transaction.get('transaction_id')
        print(f"  Linking PT {pt_id} <-> BT {bt_transaction_id}")
        if link_bank_transaction(bt_transaction_id, pt_id):
            success_count += 1
            
    print(f"  Successfully linked: {success_count}/{len(simple_matches)}")

def run_matching_for_player(player_id: int, base_output_dir: str = None, delay_seconds: int = 0):
    """Main function to run comprehensive matching for a player."""
    
    if delay_seconds > 0:
        time.sleep(delay_seconds)
    
    print(f"🚀 Starting comprehensive matching for player {player_id}")
    print("=" * 60)
    
    # Create output directory
    output_dir = create_output_directory(player_id, base_dir=base_output_dir)
    print(f"📁 Output directory: {output_dir}")
    
    # Fetch all data
    print("\n📊 Fetching transaction data...")
    platform_transactions = get_all_platform_transactions(player_id)
    bank_transactions = get_all_bank_transactions(player_id)
    scraped_transactions = get_scraped_transactions(player_id)
    
    print(f"  Platform transactions: {len(platform_transactions)}")
    print(f"  Bank transactions: {len(bank_transactions)}")
    print(f"  Scraped transactions: {len(scraped_transactions)}")
    
    # Save raw data for reference
    raw_data_dir = os.path.join(output_dir, "raw_data")
    os.makedirs(raw_data_dir, exist_ok=True)
    
    with open(os.path.join(raw_data_dir, "platform_transactions.json"), 'w') as f:
        json.dump(platform_transactions, f, indent=2)
    
    with open(os.path.join(raw_data_dir, "bank_transactions.json"), 'w') as f:
        json.dump(bank_transactions, f, indent=2)
    
    with open(os.path.join(raw_data_dir, "scraped_transactions.json"), 'w') as f:
        json.dump(scraped_transactions, f, indent=2)
    
    # Run matching
    print("\n🔍 Running PayPal to Bank matching...")
    matcher = PayPalToBankMatcher(platform_transactions, bank_transactions, scraped_transactions)
    results = matcher.match_transactions()
    
    print(f"\n📈 Matching Results:")
    print(f"  Simple matches: {len(results.simple_matches)}")
    print(f"  Three-way matches: {len(results.three_way_matches)}")
    print(f"  Unmatched: {len(results.unmatched_platform_transactions)}")
    
    # Save match results
    print("\n💾 Saving match results...")
    match_files = save_match_results(output_dir, results, player_id)
    
    print(f"  ✅ Simple matches: {match_files['simple_matches_file']}")
    print(f"  ✅ Three-way matches: {match_files['three_way_matches_file']}")
    print(f"  ✅ Unmatched analysis: {match_files['unmatched_file']}")
    print(f"  ✅ Summary report: {match_files['summary_file']}")
    
    # Execute matches
    execute_simple_matches(results.simple_matches)
    
    if results.three_way_matches:
        for match in results.three_way_matches:
            execute_three_way_match(match, player_id, bank_transactions)
    else:
        print("\n--- No Three-Way Matches to Execute ---")
    
    print(f"\n🎉 Comprehensive matching complete!")
    print(f"📁 All results saved to: {output_dir}")
    
    # Display summary
    print(f"\n📋 SUMMARY:")
    print(f"  Simple matches found: {len(results.simple_matches)}")
    print(f"  Three-way matches found: {len(results.three_way_matches)}")
    print(f"  Unmatched transactions: {len(results.unmatched_platform_transactions)}")
    
    if results.simple_matches:
        print(f"\n🔗 SIMPLE MATCHES:")
        for match in results.simple_matches:
            print(f"  PT {match.platform_transaction.get('id')} <-> BT {match.bank_transaction.get('id')}")
    
    if results.three_way_matches:
        print(f"\n🔗 THREE-WAY MATCHES:")
        for match in results.three_way_matches:
            pt_id = match.original_platform_transaction.get('id')
            bt_info = f"BT {match.paypal_bank_transaction.get('id')}" if match.paypal_bank_transaction else "(No PayPal BT Found)"
            print(f"  PT {pt_id} -> {bt_info} (fees: ${match.fee_amount})")

if __name__ == "__main__":
    # To run for multiple players, add their IDs to this list
    player_ids_to_process = [33003] 

    # --- Batch processing settings ---
    # Number of players to process concurrently.
    MAX_WORKERS = 5 
    # Delay in seconds between starting each player's task to avoid overwhelming the API.
    DELAY_BETWEEN_TASKS = 2

    total_players = len(player_ids_to_process)
    print(f"Starting batch processing for {total_players} players with {MAX_WORKERS} concurrent workers.")

    # Create a single base directory for the batch run
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    batch_base_dir = f"batch_run_bettingpaypal_{timestamp}"
    os.makedirs(batch_base_dir, exist_ok=True)
    print(f"📁 Batch output will be saved in: {batch_base_dir}")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Assign a delay to each task to stagger the API calls
        futures = [executor.submit(run_matching_for_player, player_id, batch_base_dir, i * DELAY_BETWEEN_TASKS) for i, player_id in enumerate(player_ids_to_process)]
        
        for i, future in enumerate(futures):
            future.result()  # Wait for the task to complete and handle any exceptions
            print(f"--- Completed processing player {i + 1}/{total_players} ---")

    print("\n--- Batch processing complete. ---")
