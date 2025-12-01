import requests
from datetime import datetime
from config import BANK_TRANSACTIONS_API_URL

def inspect_bank_transaction():
    # Use a player known to have transactions, e.g., 49496
    player_id = 49496
    start_date = "2020-01-01"
    end_date = datetime.now().strftime('%Y-%m-%d')
    
    params = {
        'player_id': player_id,
        'start_date': start_date,
        'end_date': end_date
    }
    
    try:
        response = requests.get(BANK_TRANSACTIONS_API_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        transactions = data.get('bankTransactions', [])
        if transactions:
            bt = transactions[0]
            print("Keys in Bank Transaction:")
            print(list(bt.keys()))
            print(f"id: {bt.get('id')} (Type: {type(bt.get('id'))})")
            print(f"transaction_id: {bt.get('transaction_id')} (Type: {type(bt.get('transaction_id'))})")
        else:
            print("No transactions found.")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    inspect_bank_transaction()
