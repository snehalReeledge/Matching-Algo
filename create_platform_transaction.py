import requests
from datetime import datetime
from config import CREATE_PLATFORM_TRANSACTION_API_URL, AI_USER_ID

def create_platform_transaction(original_transaction, changes):
    """
    Creates a new platform transaction based on an original transaction,
    applying any specified changes.
    """
    
    # Build the new payload field-by-field as requested
    new_transaction_payload = {
        "Transaction_Type": original_transaction.get("Transaction_Type"),
        "Date": original_transaction.get("Date"),
        "User_ID": original_transaction.get("User_ID"),
        "Amount": original_transaction.get("Amount"),
        "Status": original_transaction.get("Status"),
        "From_Account": original_transaction.get("From_Account"), # Will be overridden by changes
        "To_Account": original_transaction.get("To_Account"),
        "Offer": original_transaction.get("Offer"),
        "Casino": original_transaction.get("Casino"),
        "Last_Updated": int(datetime.now().timestamp() * 1000),
        "Added_By": AI_USER_ID,
        "Comments": original_transaction.get("Comments"),
    }
    
    # Apply the specified changes from the matcher logic (e.g., From_Account, related_bank_transaction)
    new_transaction_payload.update(changes)
    
    # For now, we will still print the payload for verification, but the API call is now active
    print("\n--- Create Platform Transaction ---")
    print("Payload to be sent:")
    print(new_transaction_payload)
    print("----------------------------------\n")

    try:
        response = requests.post(CREATE_PLATFORM_TRANSACTION_API_URL, json=new_transaction_payload)
        response.raise_for_status()
        print("Successfully created new platform transaction.")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Failed to create new platform transaction: {e}")
        return None

if __name__ == '__main__':
    # Example usage:
    original_pt = {
        "id": 12345,
        "User_ID": 1065,
        "Date": "2025-01-01",
        "Amount": 100,
        "From_Account": 111,
        "To_Account": 222
    }
    changes_to_apply = {
        "From_Account": 36715,
    }
    
    create_platform_transaction(original_pt, changes_to_apply)
