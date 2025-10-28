import requests
import json
from config import PLATFORM_TRANSACTIONS_API_URL, BANK_TRANSACTIONS_API_URL

def get_platform_transaction(pt_id: int):
    """Fetches a single platform transaction by its ID."""
    print(f"Fetching Platform Transaction ID: {pt_id}")
    try:
        url = f"https://xhks-nxia-vlqr.n7c.xano.io/api:1ZwRS-f0/transactions/{pt_id}"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"  Error fetching PT {pt_id}: {e}")
        return None

def get_bank_transaction(bt_id: int):
    """Fetches a single bank transaction by its ID."""
    print(f"Fetching Bank Transaction ID: {bt_id}")
    try:
        # Note: This endpoint might need adjustment if it's different for single lookups
        url = f"https://xhks-nxia-vlqr.n7c.xano.io/api:1ZwRS-f0/bank_transactions/{bt_id}"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"  Error fetching BT {bt_id}: {e}")
        return None

if __name__ == "__main__":
    PT_ID_TO_CHECK = 101825
    CURRENTLY_LINKED_BT_ID = 318983
    PROPOSED_BT_ID = 497971

    print("--- Investigating Simple Match Failure ---")
    
    pt_data = get_platform_transaction(PT_ID_TO_CHECK)
    linked_bt_data = get_bank_transaction(CURRENTLY_LINKED_BT_ID)
    proposed_bt_data = get_bank_transaction(PROPOSED_BT_ID)

    print("\n--- Results ---")

    if pt_data:
        print("\n[Platform Transaction Details]")
        print(json.dumps(pt_data, indent=2))
    else:
        print("\nCould not retrieve Platform Transaction.")

    if linked_bt_data:
        print("\n[Currently Linked Bank Transaction Details]")
        print(json.dumps(linked_bt_data, indent=2))
    else:
        print("\nCould not retrieve the currently linked Bank Transaction.")

    if proposed_bt_data:
        print("\n[Proposed Bank Transaction Details]")
        print(json.dumps(proposed_bt_data, indent=2))
    else:
        print("\nCould not retrieve the proposed Bank Transaction.")

