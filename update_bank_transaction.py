import requests
from config import UPDATE_BANK_TRANSACTIONS_API_URL

# Function to update bank transaction

def update_bank_transaction(transaction_id, platform_transaction_id):
    """
    Updates the bank transaction with the given transaction_id.
    - Sets transaction_link to platform_transaction_id if transaction_link is empty.
    - Sets last_edited_by to 35047.
    """
    # Construct the URL
    url = UPDATE_BANK_TRANSACTIONS_API_URL.format(transaction_id=transaction_id)

    # Prepare the payload
    payload = {
        "transaction_link": platform_transaction_id,
        "last_edited_by": 35047
    }

    # Send the PATCH request
    try:
        response = requests.patch(url, json=payload)
        response.raise_for_status()
        print(f"Successfully updated bank transaction {transaction_id}.")
    except requests.exceptions.RequestException as e:
        print(f"Failed to update bank transaction {transaction_id}: {e}")

# Example usage
if __name__ == "__main__":
    # Example bank transaction ID and platform transaction ID
    transaction_id = "12345"
    platform_transaction_id = "67890"
    update_bank_transaction(transaction_id, platform_transaction_id)
