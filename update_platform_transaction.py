import requests
from config import UPDATE_PLATFORM_TRANSACTIONS_API_URL

def update_platform_transaction(platform_transaction_id, payload):
    """
    Updates a platform transaction with the given payload.
    """
    url = UPDATE_PLATFORM_TRANSACTIONS_API_URL.format(platform_transaction_id=platform_transaction_id)
    try:
        response = requests.patch(url, json=payload)
        response.raise_for_status()
        print(f"Successfully updated platform transaction {platform_transaction_id}.")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Failed to update platform transaction {platform_transaction_id}: {e}")
        return None

# Example usage
if __name__ == "__main__":
    platform_transaction_id = "12345"
    payload_to_update = {"Notes": "This is a test update."}
    update_platform_transaction(platform_transaction_id, payload_to_update)
