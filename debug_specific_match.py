import requests
import argparse
from datetime import datetime
from config import PLATFORM_TRANSACTIONS_API_URL, BANK_TRANSACTIONS_API_URL
from received_transaction_matcher import SimpleReceivedTransactionMatcher

def fetch_transaction_details(pt_id, bt_id, player_id):
    """Fetches the full details for the specific platform and bank transactions."""
    try:
        # Fetch all platform transactions for the user (since we can't query by transaction ID)
        pt_response = requests.get(PLATFORM_TRANSACTIONS_API_URL, params={'user_id': player_id}, timeout=120)
        pt_response.raise_for_status()
        all_platform_transactions = pt_response.json()
        platform_transaction = next((pt for pt in all_platform_transactions if pt.get('id') == pt_id), None)

        # Fetch all bank transactions for the user
        bt_response = requests.get(BANK_TRANSACTIONS_API_URL, params={'player_id': player_id}, timeout=120)
        bt_response.raise_for_status()
        all_bank_transactions = bt_response.json().get('bankTransactions', [])
        bank_transaction = next((bt for bt in all_bank_transactions if bt.get('id') == bt_id), None)
        
        if not platform_transaction:
            print(f"Error: Platform Transaction ID {pt_id} not found for player {player_id}.")
            return None, None
        if not bank_transaction:
            print(f"Error: Bank Transaction ID {bt_id} not found for player {player_id}.")
            return None, None
            
        return platform_transaction, bank_transaction
    except requests.exceptions.RequestException as e:
        print(f"API Error: {e}")
        return None, None

def trace_match_logic(pt, bt, player_id):
    """Traces the matching logic step-by-step for a single PT and BT pair."""
    print("\n" + "="*60)
    print(f"Tracing Match: PT ID {pt.get('id')} vs. BT ID {bt.get('id')}")
    print("="*60)

    # We need a matcher instance to use its internal logic and get checkbook payments
    # Pass in minimal data since we are only using its internal helper methods
    matcher = SimpleReceivedTransactionMatcher(
        platform_transactions=[pt],
        bank_transactions=[bt],
        user_id=player_id
    )

    # --- Step 1: Core Match Criteria ---
    print("\n--- Step 1: Core Match Criteria ---")
    amount_match = matcher._is_amount_match(float(pt.get('Amount', 0)), float(bt.get('amount', 0)))
    print(f"  - Amount Match: {'✅' if amount_match else '❌'} (PT: ${pt.get('Amount')} vs BT: ${bt.get('amount')})")

    pt_date = datetime.fromisoformat(pt.get('Date', '').replace('Z', '+00:00'))
    bt_date = datetime.fromisoformat(bt.get('date', '').replace('Z', '+00:00'))
    date_match = matcher._is_date_match(pt_date, bt_date)
    print(f"  - Date Match (±5 days): {'✅' if date_match else '❌'} (PT: {pt.get('Date')} vs BT: {bt.get('date')})")

    account_match = matcher._is_bank_account_match(pt, bt)
    print(f"  - Bank Account ID Match: {'✅' if account_match else '❌'} (PT Acct: {pt.get('to', {}).get('bankaccount_id')} vs BT Acct: {bt.get('bankaccount_id')})")

    if not (amount_match and date_match and account_match):
        print("\n[!] Conclusion: Failed on core criteria. No further checks needed.")
        return

    # --- Step 2: Checkbook Payment Validation ---
    print("\n--- Step 2: Checkbook Payment Validation ---")
    checkbook_payment = matcher._find_matching_checkbook_payment(bt, pt, bypass_used_check=True)
    
    if not checkbook_payment:
        print("  - Checkbook Payment Search: ❌ (No valid checkbook payment found that matches the bank transaction)")
        print("\n[!] Conclusion: A potential bank transaction was found, but it could not be linked to a valid checkbook payment.")
        return

    print(f"  - Checkbook Payment Search: ✅ (Found potential Checkbook Payment ID: {checkbook_payment.get('id')})")
    
    # --- Step 3: Check if Already Used ---
    print("\n--- Step 3: Check if Resources Were Already Used ---")
    # To do this accurately, we need to run the full matcher to populate the 'used' sets
    full_matcher = SimpleReceivedTransactionMatcher(
        platform_transactions=requests.get(PLATFORM_TRANSACTIONS_API_URL, params={'user_id': player_id}).json(),
        bank_transactions=requests.get(BANK_TRANSACTIONS_API_URL, params={'player_id': player_id}).json().get('bankTransactions', []),
        user_id=player_id
    )
    full_matcher.match_received_transactions()

    cp_id = checkbook_payment.get('id')
    is_cp_used = cp_id in full_matcher.used_checkbook_payment_ids
    print(f"  - Is Checkbook Payment ID {cp_id} already used?: {'✅' if is_cp_used else '❌'}")

    if is_cp_used:
        conflicting_pt_id = full_matcher.used_checkbook_payment_ids[cp_id]
        print(f"    - Used by: Platform Transaction ID {conflicting_pt_id}")
        print(f"\n[!] Conclusion: A perfect match was found, but the required checkbook payment was already claimed by a higher-priority transaction.")
    else:
        print(f"\n[!] Conclusion: This pair appears to be a valid match. The reason it was not matched by the main algorithm is likely due to a higher-priority match being found for PT ID {pt.get('id')} with a different bank transaction.")

    print("\n" + "="*60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Trace the matching logic for a specific platform and bank transaction.")
    parser.add_argument("player_id", type=int, help="The ID of the player.")
    parser.add_argument("platform_transaction_id", type=int, help="The ID of the platform transaction.")
    parser.add_argument("bank_transaction_id", type=int, help="The ID of the bank transaction to check against.")
    args = parser.parse_args()

    platform_transaction, bank_transaction = fetch_transaction_details(args.platform_transaction_id, args.bank_transaction_id, args.player_id)
    
    if platform_transaction and bank_transaction:
        trace_match_logic(platform_transaction, bank_transaction, args.player_id)

