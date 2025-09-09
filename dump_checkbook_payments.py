import requests
import json
import argparse
import os
from datetime import datetime

from config import CHECKBOOK_PAYMENTS_API_URL

def fetch_and_save_checkbook_payments(player_id, start_date=None, end_date=None):
    """Fetches all checkbook payments for a player and saves them to a file."""
    
    output_dir = "debug"
    output_filename = os.path.join(output_dir, f"player_{player_id}_checkbook_payments_dump.json")

    print(f"Fetching all checkbook payments for player ID: {player_id}...")
    if start_date and end_date:
        print(f"Date Range: {start_date} to {end_date}")

    try:
        params = {'user_id': [player_id]}
        if start_date:
            params['start_date'] = start_date
        if end_date:
            params['end_date'] = end_date
            
        response = requests.get(CHECKBOOK_PAYMENTS_API_URL, params=params, timeout=120)
        response.raise_for_status()
        
        data = response.json()
        payments = data.get('checkbookPayments', [])
        
        with open(output_filename, 'w') as f:
            json.dump(payments, f, indent=4)
            
        print(f"\nSuccessfully saved {len(payments)} checkbook payments to '{output_filename}'.")
        
        # Also, print a summary to the console
        print("\n--- API Response Summary ---")
        if not payments:
            print("The API returned an empty list of payments for this player.")
        else:
            payment_ids = [p for p in payments]
            print(f"Found payment IDs: {payment_ids}")
            if 12872 in payment_ids:
                print("\n[✅] Found Checkbook Payment ID 12872 in the response.")
            else:
                print("\n[❌] Did NOT find Checkbook Payment ID 12872 in the response.")

    except requests.exceptions.RequestException as e:
        print(f"\nAPI Error: {e}")
    except json.JSONDecodeError:
        print(f"\nError: Could not decode JSON from the API response.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch and dump all checkbook payments for a specific player.")
    parser.add_argument("player_id", type=int, help="The ID of the player.")
    parser.add_argument("--start_date", type=str, help="Start date in YYYY-MM-DD format.")
    parser.add_argument("--end_date", type=str, help="End date in YYYY-MM-DD format.")
    args = parser.parse_args()
    fetch_and_save_checkbook_payments(args.player_id, args.start_date, args.end_date)

