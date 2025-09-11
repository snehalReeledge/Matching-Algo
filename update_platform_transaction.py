import requests
from config import UPDATE_PLATFORM_TRANSACTIONS_API_URL

def update_platform_transaction_date(platform_transaction_id, new_date):
    """
    Updates the platform transaction's date.
    - Sets 'Date' to the new_date value.
    """
    # Construct the URL
    url = UPDATE_PLATFORM_TRANSACTIONS_API_URL.format(platform_transaction_id=platform_transaction_id)

    # The date from the bank transaction might have timezone info, but the API might expect a clean string.
    # We will format it to just YYYY-MM-DD.
    formatted_date = new_date.split('T')[0]

    # Prepare the payload
    payload = {
        "Date": formatted_date,
    }

    # Send the PATCH request
    try:
        response = requests.patch(url, json=payload)
        response.raise_for_status()
        print(f"Successfully updated date for platform transaction {platform_transaction_id}.")
    except requests.exceptions.RequestException as e:
        print(f"Failed to update date for platform transaction {platform_transaction_id}: {e}")

# Example usage
if __name__ == "__main__":
    platform_transaction_id = "12345"
    new_date = "2025-01-15T12:00:00.000Z"
    update_platform_transaction_date(platform_transaction_id, new_date)
