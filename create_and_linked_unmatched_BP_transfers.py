#!/usr/bin/env python3
"""
Automated creation and linking of platform transactions for old, unlinked PayPal bank transfers.

This script identifies historical, unlinked bank transactions originating from a Betting PayPal account
and processes them based on a set of defined rules to ensure they are correctly represented
in the platform's transaction ledger.
"""

import requests
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import re
import time

from config import (
    PLATFORM_TRANSACTIONS_API_URL,
    BANK_TRANSACTIONS_API_URL,
    SCRAPED_TRANSACTION_API_URL,
    UPDATE_BANK_TRANSACTIONS_API_URL,
    UPDATE_PLATFORM_TRANSACTIONS_API_URL,
    CREATE_PLATFORM_TRANSACTION_API_URL,
    USER_ACCOUNTS_API_URL,
    AI_USER_ID,
    TRANSFER_ACCOUNT_ID,
    FEES_ACCOUNT_ID
)

# --- Keywords to identify PayPal transactions ---
BETTING_PAYPAL_KEYWORDS = [
    "money transfer to",
]

# Keywords to identify the incoming transaction to the Betting Bank account
BETTING_BANK_TRANSFER_KEYWORDS = [
    "^paypal transfer$",           # Exact match: 19.12%
    "paypal transfer",             # Contains: 49.54%
    "transfer.*paypal",            # Either order: 77.76%
    "^paypal",                     # Starts with PayPal: 68.32%
    "rtp.*paypal",                 # Real-time payment: 10.94%
    "instant.*paypal",             # Instant payment: 5.07%
    "paypal paypal",
    "money transfer authorized"
]

# --- Main Logic ---

def process_unmatched_paypal_transfers(player_id: int, dry_run: bool = False):
    """
    Main orchestrator function.
    Fetches trigger transactions and processes each one according to the defined logic.
    """
    print(f"Starting process for player {player_id}...")
    if dry_run:
        print("--- RUNNING IN DRY-RUN MODE: NO DATA WILL BE MODIFIED ---")

    # 1. Get Trigger Bank Transactions
    trigger_bts = get_old_unlinked_paypal_transactions(player_id)
    print(f"  Found {len(trigger_bts)} trigger bank transactions to process.")

    if not trigger_bts:
        print("  No transactions to process. Exiting.")
        return

    # Fetch all other required data once to avoid repeated API calls
    all_user_accounts = get_user_accounts(player_id)
    if not all_user_accounts:
        print("  - ERROR: Could not fetch user accounts for this player. Aborting.")
        return

    default_betting_bank_id = None
    for acc in all_user_accounts:
        if (acc.get('isDefault') and
            acc.get('Account_Type', '').lower() == 'betting bank account'):
            default_betting_bank_id = acc.get('id')
            break
    
    if not default_betting_bank_id:
        print("  - ERROR: Could not find a default Betting Bank account for this player. Aborting.")
        return

    all_scraped_transactions = get_scraped_transactions(player_id)
    all_platform_transactions = get_platform_transactions(player_id)
    all_bank_transactions = get_all_bank_transactions(player_id) # Fetch all BTs for lookups

    # --- New Filter: Exclude BTs older than the oldest PT ---
    if not all_platform_transactions:
        print("  - No platform transactions found for this player. Cannot determine date cutoff. Exiting.")
        return

    try:
        oldest_pt_date_str = min(pt['Date'] for pt in all_platform_transactions if pt.get('Date'))
        oldest_pt_date = datetime.strptime(oldest_pt_date_str, '%Y-%m-%d')
        print(f"  Oldest platform transaction date found: {oldest_pt_date.strftime('%Y-%m-%d')}")

        original_bt_count = len(trigger_bts)
        trigger_bts = [
            bt for bt in trigger_bts
            if datetime.strptime(bt.get('date'), '%Y-%m-%d') >= oldest_pt_date
        ]
        
        filtered_count = original_bt_count - len(trigger_bts)
        if filtered_count > 0:
            print(f"  - Excluded {filtered_count} bank transactions that are older than the oldest platform transaction.")
        
        if not trigger_bts:
            print("  No transactions remaining after date filter. Exiting.")
            return
    except (ValueError, TypeError) as e:
        print(f"  - ERROR: Could not parse dates to determine oldest platform transaction. Skipping date filter. Error: {e}")
    # --- End New Filter ---

    # 2. Process Each Trigger Transaction
    for bt in trigger_bts:
        print(f"\n--- Processing Trigger BT ID: {bt.get('id')} | Date: {bt.get('date')} | Amount: {bt.get('amount')} ---")
        process_single_transaction(bt, all_scraped_transactions, all_platform_transactions, all_bank_transactions, player_id, default_betting_bank_id, all_user_accounts, dry_run)

    print(f"\nProcess complete for player {player_id}.")


def process_single_transaction(bt: Dict[str, Any], scraped_txns: List[Dict[str, Any]], platform_txns: List[Dict[str, Any]], all_bts: List[Dict[str, Any]], player_id: int, default_betting_bank_id: int, user_accounts: List[Dict[str, Any]], dry_run: bool):
    """
    Contains the core logic for processing one bank transaction.
    """
    # Step 1: Find Corresponding Scraped Transaction
    scraped_match = find_scraped_transaction_match(bt, scraped_txns)
    if not scraped_match:
        print("  - No corresponding scraped transaction found. Skipping.")
        return

    print(f"  + Found Scraped Match ID: {scraped_match.get('id')} | Gross: {scraped_match.get('Gross')} | Net: {scraped_match.get('Net')}")

    st_gross = abs(float(scraped_match.get('Gross', 0)))
    st_net = abs(float(scraped_match.get('Net', 0)))
    st_date_str = scraped_match.get('Transaction Date')

    # Step 2: Handle Matched Scraped Transaction
    # Case A: Check for an existing "Gross" PT
    gross_pt_match = find_platform_transaction_match(
        amount=st_gross,
        date_str=st_date_str,
        from_account_type='betting paypal account',
        to_account_type='transfer account',
        platform_txns=platform_txns
    )
    if gross_pt_match:
        print(f"  - CASE A: Found existing 'Gross' PT ID: {gross_pt_match.get('id')}.")
        link_bank_transaction(bt.get('transaction_id'), gross_pt_match.get('id'), dry_run)
        return

    # Case B: Check for an existing "Net" PT
    net_pt_match = find_platform_transaction_match(
        amount=st_net,
        date_str=st_date_str,
        from_account_type='betting paypal account',
        to_account_type='betting bank account',
        platform_txns=platform_txns
    )

    if net_pt_match:
        # NEW VALIDATION from user feedback
        is_already_linked_to_paypal = False
        from_account_id = net_pt_match.get('from', {}).get('bankaccount_id')
        for related_bt in net_pt_match.get('related_bank_transaction', []):
            if related_bt.get('bankaccount', {}).get('id') == from_account_id:
                is_already_linked_to_paypal = True
                break
        
        if is_already_linked_to_paypal:
            print(f"  - CASE B (Skipped): Found 'Net' PT ID: {net_pt_match.get('id')}, but it's already linked to a PayPal-type BT.")
            return

        print(f"  - CASE B: Found existing 'Net' PT ID: {net_pt_match.get('id')}.")
        # Fetch full details for this PT to check its links accurately
        pt_details = get_platform_transaction_details(net_pt_match.get('id'))
        if not pt_details:
            print(f"    - ERROR: Could not fetch full details for PT {net_pt_match.get('id')}. Skipping further processing.")
            return

        # Handle splitting or linking based on fees
        handle_existing_net_pt(pt_details, scraped_match, bt, player_id, all_bts, default_betting_bank_id, dry_run)
        return

    # Case C: No existing PT found, create new ones
    print("  - CASE C: No existing PT found. Creating new transaction(s).")
    handle_no_existing_pt(scraped_match, bt, player_id, all_bts, default_betting_bank_id, user_accounts, dry_run)


def run_create_and_link_for_player(player_id: int, dry_run: bool = False, delay_seconds: int = 0):
    """Wrapper function to include a delay for concurrent execution."""
    if delay_seconds > 0:
        time.sleep(delay_seconds)
    process_unmatched_paypal_transfers(player_id, dry_run)

# --- Data Fetching Functions ---

def get_old_unlinked_paypal_transactions(player_id: int) -> List[Dict[str, Any]]:
    """
    Fetches bank transactions that are potential candidates for this process.
    """
    print("  Fetching old, unlinked PayPal bank transactions...")
    try:
        cutoff_date = datetime.now() - timedelta(days=15)
        params = {
            'player_id': player_id,
            'end_date': cutoff_date.strftime('%Y-%m-%d'),
            'unlinked_only': 'true'
        }
        response = requests.get(BANK_TRANSACTIONS_API_URL, params=params)
        response.raise_for_status()
        
        all_unlinked_from_api = response.json().get('bankTransactions', [])
        
        # Add a strict client-side filter to ensure we only process truly unlinked transactions.
        # This prevents reprocessing transactions that may be incorrectly linked.
        truly_unlinked = [
            bt for bt in all_unlinked_from_api if not bt.get('linked_transaction')
        ]
        
        if len(all_unlinked_from_api) != len(truly_unlinked):
            print(f"    - INFO: Filtered out {len(all_unlinked_from_api) - len(truly_unlinked)} BTs that were already linked.")

        # Filter for transactions with an amount greater than 2
        amount_filtered = [bt for bt in truly_unlinked if bt.get('amount', 0) > 2]

        # Filter by keywords client-side
        keyword_filtered = [
            bt for bt in amount_filtered
            if any(re.search(k, bt.get('name', '').lower()) for k in BETTING_PAYPAL_KEYWORDS)
        ]

        # Further filter to ensure a bankaccount_id is present, which is critical for processing
        final_filtered = [
            bt for bt in keyword_filtered if bt.get('bankaccount_id')
        ]

        if len(keyword_filtered) != len(final_filtered):
            print(f"    - INFO: Filtered out {len(keyword_filtered) - len(final_filtered)} BTs that were missing a bankaccount_id.")

        return final_filtered
    except (requests.RequestException, ValueError) as e:
        print(f"  ERROR fetching bank transactions: {e}")
        return []

def get_user_accounts(player_id: int) -> List[Dict[str, Any]]:
    """
    Fetches all of the user's accounts from the API.
    """
    print("  Fetching user accounts...")
    try:
        params = {'user_id': player_id}
        response = requests.get(USER_ACCOUNTS_API_URL, params=params)
        response.raise_for_status()
        accounts = response.json()
        print(f"    + Found {len(accounts)} user accounts.")
        return accounts
    except (requests.RequestException, ValueError) as e:
        print(f"  ERROR fetching user accounts: {e}")
        return []

def get_default_betting_bank_account_id(player_id: int) -> Optional[int]:
    """
    Fetches the user's accounts and returns the ID of the default Betting Bank account.
    """
    print("  Fetching user accounts to find default Betting Bank...")
    try:
        params = {'user_id': player_id}
        response = requests.get(USER_ACCOUNTS_API_URL, params=params)
        response.raise_for_status()
        accounts = response.json()
        
        for acc in accounts:
            if (acc.get('isDefault') and 
                acc.get('Account_Type', '').lower() == 'betting bank account'):
                print(f"    + Found default Betting Bank account ID: {acc.get('id')}")
                return acc.get('id')
        
        print("    - WARNING: No default Betting Bank account found for this user.")
        return None
    except (requests.RequestException, ValueError) as e:
        print(f"  ERROR fetching user accounts: {e}")
        return None

def get_all_bank_transactions(player_id: int) -> List[Dict[str, Any]]:
    """Fetches all bank transactions for a given player."""
    print("  Fetching all bank transactions for linking...")
    try:
        params = {'player_id': player_id}
        response = requests.get(BANK_TRANSACTIONS_API_URL, params=params)
        response.raise_for_status()
        return response.json().get('bankTransactions', [])
    except (requests.RequestException, ValueError) as e:
        print(f"  ERROR fetching all bank transactions: {e}")
        return []

def get_platform_transaction_details(pt_id: int) -> Optional[Dict[str, Any]]:
    """Fetches the full details for a single platform transaction."""
    try:
        # Construct the URL from the update endpoint, which is the correct base for a single transaction
        base_url = UPDATE_PLATFORM_TRANSACTIONS_API_URL.rsplit('/', 1)[0]
        url = f"{base_url}/{pt_id}"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except (requests.RequestException, ValueError) as e:
        print(f"  ERROR fetching details for PT {pt_id}: {e}")
        return None

def get_scraped_transactions(player_id: int) -> List[Dict[str, Any]]:
    """Fetches all scraped transactions for a player."""
    try:
        params = {'user_id': player_id}
        response = requests.get(SCRAPED_TRANSACTION_API_URL, params=params)
        response.raise_for_status()
        return response.json()
    except (requests.RequestException, ValueError) as e:
        print(f"  ERROR fetching scraped transactions: {e}")
        return []

def get_platform_transactions(player_id: int) -> List[Dict[str, Any]]:
    """Fetches all platform transactions for a player."""
    try:
        params = {'user_id': player_id}
        response = requests.get(PLATFORM_TRANSACTIONS_API_URL, params=params)
        response.raise_for_status()
        return response.json()
    except (requests.RequestException, ValueError) as e:
        print(f"  ERROR fetching platform transactions: {e}")
        return []


# --- Helper & Action Functions ---

def find_scraped_transaction_match(bt: Dict[str, Any], scraped_txns: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Finds a matching scraped transaction for a given bank transaction."""
    bt_amount = abs(bt.get('amount', 0))
    bt_date = datetime.strptime(bt.get('date'), '%Y-%m-%d')

    for st in scraped_txns:
        # --- More flexible validation ---
        # Allow type to be 'transfer_sent' or None, but not other definite types
        st_type = st.get('type', '').lower() if st.get('type') else 'none'
        is_valid_type = st_type in ['transfer_sent', 'none']

        # Allow source to be 'paypal' or None
        st_source = st.get('source', '').lower() if st.get('source') else 'none'
        is_valid_source = st_source in ['paypal', 'none']
        
        if not (is_valid_type and is_valid_source):
            continue
        # --- End validation ---

        # Gracefully handle cases where 'Gross' might be None
        gross_amount_str = st.get('Gross')
        if gross_amount_str is None:
            continue

        st_gross = abs(float(gross_amount_str))
        # Round both amounts to 2 decimal places for a safe comparison
        if round(st_gross, 2) == round(bt_amount, 2):
            st_date = datetime.strptime(st.get('Transaction Date'), '%Y-%m-%d')
            if abs((st_date - bt_date).days) <= 3:
                return st
    return None

def find_platform_transaction_match(amount: float, date_str: str, from_account_type: str, to_account_type: str, platform_txns: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Finds a matching platform transaction based on a set of criteria."""
    target_date = datetime.strptime(date_str, '%Y-%m-%d')
    
    for pt in platform_txns:
        # Round the amount from the platform transaction for a safe comparison
        pt_amount = round(pt.get('Amount', 0), 2)
        
        if (pt_amount == round(amount, 2) and
            pt.get('from', {}).get('Account_Type', '').lower() == from_account_type and
            pt.get('to', {}).get('Account_Type', '').lower() == to_account_type):
            
            pt_date = datetime.strptime(pt.get('Date'), '%Y-%m-%d')
            if abs((pt_date - target_date).days) <= 7:
                return pt
    return None


def handle_existing_net_pt(pt: Dict[str, Any], st: Dict[str, Any], bt: Dict[str, Any], player_id: int, all_bts: List[Dict[str, Any]], default_betting_bank_id: int, dry_run: bool):
    """Logic for Case B: when a 'Net' PT is found."""
    st_gross = abs(float(st.get('Gross', 0)))
    st_net = abs(float(st.get('Net', 0)))
    fee_amount = round(st_gross - st_net, 2)

    if fee_amount < 0.01:
        # No fee, just link the transaction
        print("    - No fee detected. Linking existing PT to trigger BT.")
        link_bank_transaction(bt.get('transaction_id'), pt.get('id'), dry_run)
    else:
        # Fee exists, perform the split
        print(f"    - Fee of ${fee_amount} detected. Splitting transaction.")
        
        original_from_account_id = pt.get('From_Account')
        
        # 1. Update original PT to be the "Net" leg
        update_payload = {
            'From_Account': TRANSFER_ACCOUNT_ID,
            'Notes': f"System Match (Split): From account updated to Transfer Account on {datetime.now().isoformat()}"
        }
        update_platform_transaction(pt.get('id'), update_payload, dry_run)

        # 2. Create "Fee" PT
        fees_payload = {
            'Transaction_Type': 'fees',
            'Amount': round(fee_amount, 2),
            'Date': pt.get('Date'),
            'From_Account': TRANSFER_ACCOUNT_ID,
            'To_Account': FEES_ACCOUNT_ID,
            'User_ID': player_id,
            'Added_By': AI_USER_ID,
            'Status': 'Completed'
        }
        create_platform_transaction(fees_payload, dry_run)

        # 3. Create "Gross" PT and link it
        gross_payload = {
            'Transaction_Type': 'transfer',
            'Amount': round(st_gross, 2),
            'Date': pt.get('Date'),
            'From_Account': original_from_account_id,
            'To_Account': TRANSFER_ACCOUNT_ID,
            'User_ID': player_id,
            'Added_By': AI_USER_ID,
            'Status': 'Completed'
        }
        new_gross_pt = create_platform_transaction(gross_payload, dry_run)
        if new_gross_pt:
            link_bank_transaction(bt.get('transaction_id'), new_gross_pt.get('id'), dry_run)
        
        # Per user feedback, the bank-side deposit is presumed to be linked already in Case B.
        # The purpose of this logic is to fix the withdrawal side and account for fees.
        # Therefore, we will no longer search for the bank deposit link here.


def find_matching_bank_deposit(net_amount: float, net_date_str: str, all_bts: List[Dict[str, Any]], user_accounts: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Searches for a single, unambiguous bank deposit matching the criteria across all of a user's betting bank accounts.
    """
    net_date = datetime.strptime(net_date_str, '%Y-%m-%d')

    # Get all possible bank account IDs for this user's betting banks
    betting_bank_account_ids = [
        acc.get('bankaccount_id') for acc in user_accounts 
        if acc.get('Account_Type', '').lower() == 'betting bank account' and acc.get('bankaccount_id')
    ]

    if not betting_bank_account_ids:
        return None

    found_deposits = []
    for bt in all_bts:
        if (not bt.get('linked_transaction') and
            bt.get('bankaccount_id') in betting_bank_account_ids and
            round(bt.get('amount', 0), 2) == -round(net_amount, 2)):
            
            bt_date = datetime.strptime(bt.get('date'), '%Y-%m-%d')
            if abs((bt_date - net_date).days) <= 7:
                description = bt.get('name', '').lower()
                if any(re.search(k, description) for k in BETTING_BANK_TRANSFER_KEYWORDS):
                    found_deposits.append(bt)
    
    # Return the transaction only if one unique match is found
    if len(found_deposits) == 1:
        return found_deposits[0]
    
    return None


def handle_no_existing_pt(st: Dict[str, Any], bt: Dict[str, Any], player_id: int, all_bts: List[Dict[str, Any]], default_betting_bank_id: int, user_accounts: List[Dict[str, Any]], dry_run: bool):
    """Logic for Case C: when no existing PT is found."""
    st_gross = abs(float(st.get('Gross', 0)))
    st_net = abs(float(st.get('Net', 0)))
    fee_amount = round(st_gross - st_net, 2)
    
    # We need the Betting PayPal account ID to create the new transaction.
    # First, get the bankaccount_id from the trigger bank transaction.
    trigger_bankaccount_id = bt.get('bankaccount_id')
    if not trigger_bankaccount_id:
        print("    - ERROR: Trigger bank transaction is missing 'bankaccount_id'. Skipping.")
        return

    # Now, find the User Account that corresponds to this bankaccount_id.
    betting_paypal_account_id = None
    for acc in user_accounts:
        if acc.get('bankaccount_id') == trigger_bankaccount_id:
            betting_paypal_account_id = acc.get('id')
            break
    
    if not betting_paypal_account_id:
        print(f"    - ERROR: Could not find a User Account linked to bankaccount_id {trigger_bankaccount_id}. Skipping.")
        return

    # --- New Logic: Find deposit FIRST to determine destination account ---
    net_amount_to_find = st_net if fee_amount >= 0.01 else st_gross
    matched_deposit_bt = find_matching_bank_deposit(net_amount_to_find, st.get('Transaction Date'), all_bts, user_accounts)

    to_account_id = default_betting_bank_id # Fallback to default
    if matched_deposit_bt:
        print(f"    - Found unambiguous bank deposit BT ID {matched_deposit_bt.get('id')}. Using its account as the destination.")
        deposit_bankaccount_id = matched_deposit_bt.get('bankaccount_id')
        # Find the platform account ID for this bankaccount_id
        for acc in user_accounts:
            if acc.get('bankaccount_id') == deposit_bankaccount_id:
                to_account_id = acc.get('id')
                break
    else:
        print("    - Could not find an unambiguous bank deposit. Falling back to default betting bank for 'To' account.")
    # --- End New Logic ---

    if fee_amount < 0.01:
        # No fee, create a single new PT and link it
        print("    - No fee detected. Creating a new transfer PT.")
        payload = {
            'Transaction_Type': 'transfer',
            'Amount': round(st_gross, 2),
            'Date': st.get('Transaction Date'),
            'From_Account': betting_paypal_account_id,
            'To_Account': to_account_id,
            'User_ID': player_id,
            'Added_By': AI_USER_ID,
            'Status': 'Completed'
        }
        new_pt = create_platform_transaction(payload, dry_run)
        if new_pt:
            link_bank_transaction(bt.get('transaction_id'), new_pt.get('id'), dry_run)
            if matched_deposit_bt:
                link_bank_transaction(matched_deposit_bt.get('transaction_id'), new_pt.get('id'), dry_run)

    else:
        # Fee exists, create the full split
        print(f"    - Fee of ${fee_amount} detected. Creating split transactions.")
        
        # 1. "Gross" PT (linked to trigger BT)
        gross_payload = {
            'Transaction_Type': 'transfer',
            'Amount': round(st_gross, 2),
            'Date': st.get('Transaction Date'),
            'From_Account': betting_paypal_account_id,
            'To_Account': TRANSFER_ACCOUNT_ID,
            'User_ID': player_id,
            'Added_By': AI_USER_ID,
            'Status': 'Completed'
        }
        new_gross_pt = create_platform_transaction(gross_payload, dry_run)
        if new_gross_pt:
            link_bank_transaction(bt.get('transaction_id'), new_gross_pt.get('id'), dry_run)

        # 2. "Fee" PT
        fees_payload = {
            'Transaction_Type': 'fees',
            'Amount': round(fee_amount, 2),
            'Date': st.get('Transaction Date'),
            'From_Account': TRANSFER_ACCOUNT_ID,
            'To_Account': FEES_ACCOUNT_ID,
            'User_ID': player_id,
            'Added_By': AI_USER_ID,
            'Status': 'Completed'
        }
        create_platform_transaction(fees_payload, dry_run)

        # 3. "Net" PT
        net_payload = {
            'Transaction_Type': 'transfer',
            'Amount': round(st_net, 2),
            'Date': st.get('Transaction Date'),
            'From_Account': TRANSFER_ACCOUNT_ID,
            'To_Account': to_account_id,
            'User_ID': player_id,
            'Added_By': AI_USER_ID,
            'Status': 'Completed'
        }
        new_net_pt = create_platform_transaction(net_payload, dry_run)
        if new_net_pt and matched_deposit_bt:
            link_bank_transaction(matched_deposit_bt.get('transaction_id'), new_net_pt.get('id'), dry_run)


def link_bank_transaction(bank_transaction_id: str, platform_transaction_id: int, dry_run: bool):
    """Links a bank transaction to a platform transaction."""
    if dry_run:
        print(f"    [DRY RUN] Would link BT {bank_transaction_id} to PT {platform_transaction_id}")
        return

    url = UPDATE_BANK_TRANSACTIONS_API_URL.format(transaction_id=bank_transaction_id)
    payload = {"transaction_link": platform_transaction_id, "last_edited_by": AI_USER_ID}
    try:
        response = requests.patch(url, json=payload, timeout=10)
        response.raise_for_status()
        print(f"    - LINK SUCCESS: BT {bank_transaction_id} -> PT {platform_transaction_id}")
    except requests.RequestException as e:
        print(f"    - LINK FAILED for BT {bank_transaction_id}: {e}")

def update_platform_transaction(pt_id: int, changes: dict, dry_run: bool):
    """Updates an existing platform transaction."""
    if dry_run:
        print(f"    [DRY RUN] Would update PT {pt_id} with changes: {changes}")
        return

    url = UPDATE_PLATFORM_TRANSACTIONS_API_URL.format(platform_transaction_id=pt_id)
    try:
        response = requests.patch(url, json=changes, timeout=10)
        response.raise_for_status()
        print(f"    - UPDATE SUCCESS: PT {pt_id}")
        return response.json()
    except requests.RequestException as e:
        print(f"    - UPDATE FAILED for PT {pt_id}: {e}")
        return None

def find_and_link_net_bank_transaction(net_pt: Dict[str, Any], all_bts: List[Dict[str, Any]], betting_bank_id: int, dry_run: bool):
    """
    Finds the bank-side deposit transaction corresponding to a 'Net' transfer and links it.
    """
    if not net_pt:
        return

    print(f"    - Searching for corresponding bank deposit for Net PT {net_pt.get('id')}...")
    net_amount = -round(net_pt.get('Amount', 0), 2) # Bank deposits are negative
    net_date = datetime.strptime(net_pt.get('Date'), '%Y-%m-%d')
    
    # Find unlinked bank transaction that matches
    for bt in all_bts:
        if (not bt.get('linked_transaction') and
            bt.get('bankaccount_id') == betting_bank_id and
            round(bt.get('amount', 0), 2) == net_amount):
            
            bt_date = datetime.strptime(bt.get('date'), '%Y-%m-%d')
            if abs((bt_date - net_date).days) <= 7:
                description = bt.get('name', '').lower()
                if any(re.search(k, description) for k in BETTING_BANK_TRANSFER_KEYWORDS):
                    print(f"      + Found matching bank deposit BT ID: {bt.get('id')}. Linking.")
                    link_bank_transaction(bt.get('transaction_id'), net_pt.get('id'), dry_run)
                    return # Stop after finding the first match
    
    print("      - No matching bank deposit found to link.")


def create_platform_transaction(payload: dict, dry_run: bool):
    """Creates a new platform transaction."""
    if dry_run:
        print(f"    [DRY RUN] Would create new PT with payload: {payload}")
        # For dry runs, return a mock object so that linking can be tested.
        # Include the Date so that downstream functions can use it.
        return {'id': 'dry_run_pt_id', 'Date': payload.get('Date'), 'Amount': payload.get('Amount')}

    try:
        response = requests.post(CREATE_PLATFORM_TRANSACTION_API_URL, json=payload, timeout=10)
        response.raise_for_status()
        new_pt = response.json()
        print(f"    - CREATE SUCCESS: New PT ID {new_pt.get('id')} ({payload.get('Transaction_Type')}, ${payload.get('Amount')})")
        return new_pt
    except requests.RequestException as e:
        print(f"    - CREATE FAILED for new {payload.get('Transaction_Type')} transaction: {e}")
        return None

# --- Main Execution Block ---

if __name__ == "__main__":
    import argparse
    import time
    from concurrent.futures import ThreadPoolExecutor

    parser = argparse.ArgumentParser(description="Create and link platform transactions for old, unlinked PayPal bank transfers.")
    parser.add_argument("player_ids", nargs='+', type=int, help="One or more player IDs to process.")
    parser.add_argument("--dry-run", action="store_true", help="Run the script in read-only mode without making any changes.")
    parser.add_argument("--max-workers", type=int, default=5, help="Maximum number of concurrent players to process.")
    parser.add_argument("--delay", type=int, default=2, help="Delay in seconds between starting each player's task.")
    
    args = parser.parse_args()

    if args.dry_run:
        print("\n--- RUNNING IN DRY-RUN MODE: NO DATA WILL BE MODIFIED ---\n")

    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = [
            executor.submit(run_create_and_link_for_player, pid, args.dry_run, i * args.delay)
            for i, pid in enumerate(args.player_ids)
        ]
        for i, future in enumerate(futures):
            future.result()  # Wait for completion and handle exceptions
            print(f"--- Completed processing player {i + 1}/{len(args.player_ids)} ---")

    print("\n--- Batch processing complete. ---")
