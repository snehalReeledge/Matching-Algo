import requests
from datetime import datetime, timedelta
from config import (
    PLATFORM_TRANSACTIONS_API_URL, 
    BANK_TRANSACTIONS_API_URL,
    UPDATE_BANK_TRANSACTIONS_API_URL,
    AI_USER_ID,
    FEES_ACCOUNT_ID
)

# Define keyword lists for matching
BETTING_BANK_TO_FEES_KEYWORDS = [
    'MONTHLY SERVICE FEE',
    'OVERDRAFT',
    'CHECKBOOK INC',
    'PAYPAL ACCTVERIFY',
    'SIGHTLINE_SUTTON',
    'HSAWCSPCUSTODIAN ACCTVERIFY'
]

FEES_TO_BETTING_BANK_KEYWORDS = [
    'Checkbook Inc MICRO DEP',
    'CHECKBOOK INC ACCTVERIFY',
    'PAYPAL ACCTVERIFY',
    'SIGHTLINE_BNKGEO ACCOUNTREG',
    'SIGHTLINE_SUTTON',
    'HSAWCSPCUSTODIAN ACCTVERIFY'
]

def get_unmatched_fees_platform_transactions(player_id):
    """
    Retrieves unmatched 'Fees' type platform transactions for a player.
    """
    try:
        params = {'user_id': player_id}
        response = requests.get(PLATFORM_TRANSACTIONS_API_URL, params=params)
        response.raise_for_status()
        transactions = response.json()
        
        # Filter for 'Fees' type and unmatched transactions
        return [
            pt for pt in transactions
            if pt.get('Transaction_Type', '').lower() == 'fees' and not pt.get('related_bank_transaction')
        ]
    except (requests.RequestException, ValueError) as e:
        print(f"Error fetching platform transactions for player {player_id}: {e}")
        return []

def get_unmatched_bank_transactions(player_id):
    """
    Retrieves ALL unmatched bank transactions for a player.
    """
    try:
        # The BANK_TRANSACTIONS_API_URL expects 'player_id'.
        params = {'player_id': player_id}
        response = requests.get(BANK_TRANSACTIONS_API_URL, params=params)
        response.raise_for_status()
        transactions = response.json().get('bankTransactions', [])
        
        # Filter for unmatched transactions only, using 'linked_transactions' field per user instruction
        return [
            bt for bt in transactions if not bt.get('linked_transactions')
        ]
    except (requests.RequestException, ValueError) as e:
        print(f"Error fetching bank transactions for player {player_id}: {e}")
        return []

def link_transactions(platform_transaction_id, bank_transaction):
    """
    Links a bank transaction to a platform transaction.
    Uses the 'transaction_id' from the bank transaction object for the API endpoint.
    """
    bank_transaction_id_for_api = bank_transaction.get('transaction_id')
    if not bank_transaction_id_for_api:
        print(f"ERROR: Bank transaction (ID: {bank_transaction.get('id')}) is missing the required 'transaction_id' for linking.")
        return None

    url = UPDATE_BANK_TRANSACTIONS_API_URL.format(transaction_id=bank_transaction_id_for_api)
    payload = {"transaction_link": platform_transaction_id, "last_edited_by": AI_USER_ID}
    try:
        response = requests.patch(url, json=payload)
        response.raise_for_status()
        print(f"Successfully linked BT {bank_transaction_id_for_api} to PT {platform_transaction_id}")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Failed to link BT {bank_transaction_id_for_api}: {e}")
        return None

def log_potential_match(platform_transaction, bank_transaction):
    """
    Logs a potential match for a dry run.
    """
    print("\n" + "="*50)
    print(f"[DRY RUN] Found potential match:")
    print(f"  - Platform Transaction ID: {platform_transaction.get('id')}")
    print(f"    - Date: {platform_transaction.get('Date')}, Amount: {platform_transaction.get('Amount')}")
    print(f"    - From: {platform_transaction.get('from', {}).get('Account_Name')}, To: {platform_transaction.get('to', {}).get('Account_Name')}")
    print(f"  - Bank Transaction ID: {bank_transaction.get('id')}")
    print(f"    - Date: {bank_transaction.get('date')}, Amount: {bank_transaction.get('amount')}")
    print(f"    - Name: '{bank_transaction.get('name')}', Counterparty: '{bank_transaction.get('counterparty_name')}'")
    print("="*50)

def contains_keyword(text, keywords):
    """
    Checks if a string contains any of the keywords.
    """
    if not isinstance(text, str):
        return False
    return any(keyword in text for keyword in keywords)


def match_fee_transactions_for_player(player_id, dry_run=False):
    """
    Matches fee transactions for a single player.
    If dry_run is True, it only logs potential matches without linking them.
    """
    # print(f"DEBUG: Fetching unmatched 'Fees' platform transactions for player {player_id}...")
    platform_transactions = get_unmatched_fees_platform_transactions(player_id)
    # print(f"DEBUG: Found {len(platform_transactions)} platform transactions.")

    # print(f"DEBUG: Fetching ALL unmatched bank transactions for player {player_id}...")
    bank_transactions = get_unmatched_bank_transactions(player_id)
    # print(f"DEBUG: Found {len(bank_transactions)} bank transactions.")

    if not platform_transactions or not bank_transactions:
        print(f"No transactions to match for player {player_id} (either platform or bank transactions are missing).")
        return 0

    match_count = 0
    for pt in platform_transactions:
        # print(f"\nDEBUG: --- Processing PT ID: {pt.get('id')} | Amount: {pt.get('Amount')} | Date: {pt.get('Date')} ---")
        # print(f"DEBUG: Full PT data: {pt}")  # Print the full transaction data
        found_match = None
        
        try:
            pt_date_str = pt.get('Date')
            if not pt_date_str: continue
            pt_date = datetime.strptime(pt_date_str, '%Y-%m-%d')
        except ValueError:
            print(f"Could not parse date for PT {pt.get('id')}")
            continue

        # Direction 1: Betting Bank -> Fees
        if pt.get('from', {}).get('Account_Type', '').lower() == 'betting bank account' and \
           pt.get('To_Account') == FEES_ACCOUNT_ID:
            
            target_bank_account_id = pt.get('from', {}).get('bankaccount_id')
            if not target_bank_account_id:
                # print(f"DEBUG: Skipping PT {pt.get('id')} because it's missing a 'from' bankaccount_id.")
                continue

            # print(f"DEBUG: Direction: Betting Bank -> Fees. Target Bank Account ID: {target_bank_account_id}. Expected BT amount: {pt.get('Amount')}")
            for bt in bank_transactions:
                if bt.get('bankaccount_id') != target_bank_account_id:
                    continue
                # print(f"DEBUG:   - Evaluating BT ID: {bt.get('id')} | Amount: {bt.get('amount')} | Date: {bt.get('date')}")
                try:
                    bt_date_str = bt.get('date')
                    if not bt_date_str: continue
                    bt_date = datetime.strptime(bt_date_str, '%Y-%m-%d')
                except ValueError:
                    continue

                # Date difference check
                if abs(pt_date - bt_date) > timedelta(days=9):
                    # print(f"DEBUG:     - FAILED: Date difference is > 9 days.")
                    continue

                # Exact positive amount match
                if bt.get('amount') != pt.get('Amount'):
                    # print(f"DEBUG:     - FAILED: Amount does not match.")
                    continue
                
                # Keyword match in name or counterparty_name
                bt_name = bt.get('name', '')
                bt_counterparty = bt.get('counterparty_name', '')
                if not (contains_keyword(bt_name, BETTING_BANK_TO_FEES_KEYWORDS) or \
                   contains_keyword(bt_counterparty, BETTING_BANK_TO_FEES_KEYWORDS)):
                    # print(f"DEBUG:     - FAILED: No keyword match in name='{bt_name}' or counterparty='{bt_counterparty}'.")
                    continue
                
                # print("DEBUG:     - SUCCESS: All criteria met.")
                if found_match:
                    # More than one match found, flag for manual review
                    print(f"Multiple matches found for PT {pt.get('id')}. Flagging for review.")
                    found_match = None
                    break
                found_match = bt
            
            if found_match:
                if dry_run:
                    log_potential_match(pt, found_match)
                else:
                    link_transactions(pt.get('id'), found_match)
                match_count += 1
                bank_transactions.remove(found_match)


        # Direction 2: Fees -> Betting Bank
        elif pt.get('From_Account') == FEES_ACCOUNT_ID and \
             pt.get('to', {}).get('Account_Type', '').lower() == 'betting bank account':

            target_bank_account_id = pt.get('to', {}).get('bankaccount_id')
            if not target_bank_account_id:
                # print(f"DEBUG: Skipping PT {pt.get('id')} because it's missing a 'to' bankaccount_id.")
                continue

            # print(f"DEBUG: Direction: Fees -> Betting Bank. Target Bank Account ID: {target_bank_account_id}. Expected BT amount: {-pt.get('Amount')}")
            for bt in bank_transactions:
                if bt.get('bankaccount_id') != target_bank_account_id:
                    continue
                # print(f"DEBUG:   - Evaluating BT ID: {bt.get('id')} | Amount: {bt.get('amount')} | Date: {bt.get('date')}")
                try:
                    bt_date_str = bt.get('date')
                    if not bt_date_str: continue
                    bt_date = datetime.strptime(bt_date_str, '%Y-%m-%d')
                except ValueError:
                    continue

                # Date difference check
                if abs(pt_date - bt_date) > timedelta(days=9):
                    # print(f"DEBUG:     - FAILED: Date difference is > 9 days.")
                    continue
                
                # Exact negative amount match
                if bt.get('amount') != -pt.get('Amount'):
                    # print(f"DEBUG:     - FAILED: Amount does not match.")
                    continue
                
                # Keyword match
                bt_name = bt.get('name', '')
                bt_counterparty = bt.get('counterparty_name', '')
                if not (contains_keyword(bt_name, FEES_TO_BETTING_BANK_KEYWORDS) or \
                   contains_keyword(bt_counterparty, FEES_TO_BETTING_BANK_KEYWORDS)):
                    # print(f"DEBUG:     - FAILED: No keyword match in name='{bt_name}' or counterparty='{bt_counterparty}'.")
                    continue
                
                # print("DEBUG:     - SUCCESS: All criteria met.")
                if found_match:
                    # More than one match found, flag for manual review
                    print(f"Multiple matches found for PT {pt.get('id')}. Flagging for review.")
                    found_match = None
                    break
                found_match = bt

            if found_match:
                if dry_run:
                    log_potential_match(pt, found_match)
                else:
                    link_transactions(pt.get('id'), found_match)
                match_count += 1
                bank_transactions.remove(found_match)
    
    return match_count


def main():
    """
    Main function to drive the matching process.
    """
    # This would typically loop through all players that need matching
    # For demonstration, we can use a sample player
    sample_player_id = 40
    sample_betting_bank_id = 42
    
    print(f"Starting fee matching for player {sample_player_id}...")
    match_count = match_fee_transactions_for_player(sample_player_id)
    print(f"Fee matching process complete. Linked {match_count} transactions.")


if __name__ == "__main__":
    main()
