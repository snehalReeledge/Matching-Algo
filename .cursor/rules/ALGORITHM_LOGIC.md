# Transaction Matching Algorithm Logic

## Overview
This document explains the core logic of both the **Received** and **Returned** Transaction Matching Algorithms, which match platform transactions with bank transactions based on specific criteria.

## 1. Received Transaction Matching Algorithm

### Overview
The **Received Transaction Matching Algorithm** matches platform transactions where funds are coming INTO the platform (Transaction_Type == 'received') with corresponding bank transactions and checkbook payments.

### Core Algorithm Flow

#### 1.1 Platform Transaction Filtering
**Filter**: Platform transactions are filtered by `Transaction_Type == 'received'`
- **Purpose**: Identify transactions that represent funds coming into the platform
- **Field**: Uses the `Transaction_Type` field from platform transaction data
- **Result**: Creates a list of received platform transactions to match

#### 1.2 Bank Transaction Filtering
**Filter**: Bank transactions are filtered by negative amounts
- **Amount Filter**: Only includes transactions with negative amounts (`amount < 0`)
- **Purpose**: Identify bank transactions that represent funds being received (negative amounts indicate money coming in)

#### 1.3 Checkbook Payment Validation
**Filter**: Checkbook payments must meet specific criteria:
- **Recipient**: Must match the user_id
- **Direction**: Must be `'OUTGOING'` (funds going out from checkbook to platform)
- **Description**: Must contain "fund" (case-insensitive) OR start with "F"
- **Amount**: Must match the bank transaction amount (within $0.01 tolerance)

#### 1.4 Matching Logic
**Criteria**: Match received platform transactions with potential bank transactions based on:
1. **Amount Match**: Transaction amounts must be identical (within $0.01 tolerance)
2. **Date Match**: Transaction dates must be within 2 days of each other
3. **Bank Account Match**: Platform transaction's `to_account.bankaccount_id` must match bank transaction's `bankaccount_id`
4. **Checkbook Payment Exists**: Valid checkbook payment must be found for the transaction
5. **Date Prioritization**: Exact date matches are prioritized over closest date matches

#### 1.5 Date Prioritization Logic
**Priority Order**:
1. **First Priority**: Exact date matches (same day)
2. **Second Priority**: Closest date within ±2 days (only if no exact match exists)

**Purpose**: Ensures transactions are matched to their intended bank transactions rather than just the closest date match.

### Example Match

#### Platform Transaction (Received)
```json
{
  "id": 36221,
  "Transaction_Type": "received",
  "Amount": 1000,
  "Date": "2024-10-01",
  "from": {
    "Account_Name": "Checkbook Custodial",
    "Account_Type": "External account",
    "bankaccount_id": 99999
  },
  "to": {
    "Account_Name": "Player 15121 Account",
    "Account_Type": "Player bank account",
    "bankaccount_id": 1313
  }
}
```

#### Bank Transaction (Potential Match)
```json
{
  "id": 158411,
  "name": "DEPOSIT FROM CHECKBOOK",
  "amount": -1000,
  "date": "2024-10-01",
  "Account_Type": "EVERYDAY CHECKING ...7034",
  "bankaccount_id": 1313
}
```

#### Checkbook Payment (Validation)
```json
{
  "id": 5970,
  "amount": 1000,
  "description": "Funds",
  "direction": "OUTGOING",
  "recipient": 15121,
  "date": 1696173815000
}
```

#### Why They Match
1. ✅ **Transaction Type**: Platform transaction has `Transaction_Type == 'received'`
2. ✅ **Amount**: Both transactions are exactly $1,000 (within $0.01 tolerance)
3. ✅ **Date**: Both transactions occurred on 2024-10-01 (exact date match)
4. ✅ **Negative Amount**: Bank transaction amount < 0 (funds coming in)
5. ✅ **Bank Account ID**: Platform transaction's `to_account.bankaccount_id` matches bank transaction's `bankaccount_id`
6. ✅ **Checkbook Payment**: Valid checkbook payment found with correct criteria
7. ✅ **Date Priority**: Exact date match takes priority over closest date

## 2. Returned Transaction Matching Algorithm

### Overview
The **Returned Transaction Matching Algorithm** matches platform transactions where funds are being returned FROM the platform (Transaction_Type == 'returned') with corresponding bank transactions based on keywords.

### Update Bank Transactions

The `UPDATE_BANK_TRANSACTIONS_API_URL` is used to update specific columns of a bank transaction. For this algorithm, it updates:
- `transaction_link`: The matched platform transaction ID.
- `last_edited_by`: Always set to 35047, representing the AI user ID.

### Core Algorithm Flow

#### 2.1 Platform Transaction Filtering
**Filter**: Platform transactions are filtered by `Transaction_Type == 'returned'`
- **Purpose**: Identify transactions that represent returned funds in the platform
- **Field**: Uses the `Transaction_Type` field from platform transaction data
- **Result**: Creates a list of returned platform transactions to match

#### 2.2 Bank Transaction Filtering
**Filter**: Bank transactions are filtered by keywords and positive amounts
- **Keywords**: `["checkbook", "reel ventures", "rv enhanced wall", "individual"]`
- **Field**: Searches in the `name` field of bank transactions
- **Amount Filter**: Only includes transactions with positive amounts (`amount > 0`)
- **Purpose**: Identify bank transactions that are potential matches for returned funds

#### 2.3 Matching Logic
**Criteria**: Match returned platform transactions with potential bank transactions based on:
1. **Amount Match**: Transaction amounts must be identical (within $0.01 tolerance)
2. **Date Match**: Transaction dates must be within 5 days of each other
3. **Keyword Match**: Bank transaction must contain one of the specified keywords
4. **User Match**: Transactions must belong to the same player
5. **Bank Account Match**: Platform transaction's `from_account.bankaccount_id` must match bank transaction's `bankaccount_id`
6. **Existing Link**: If a linked transaction is already present, the match is kept as it is.

#### 2.4 Example Match

##### Platform Transaction (Returned)
```json
{
  "id": 170976,
  "Transaction_Type": "returned",
  "Amount": 4690,
  "Date": "2025-08-05",
  "from": {
    "Account_Name": "Betting bank",
    "Account_Type": "Betting bank account",
    "bankaccount_id": 18668
  },
  "to": {
    "Account_Name": "Checkbook Custodial",
    "Account_Type": "Backer bank account",
    "bankaccount_id": 1313
  }
}
```

##### Bank Transaction (Potential Match)
```json
{
  "id": 484560,
  "name": "D15121 INDIVIDUAL 049468200001508 CHECK 5011 KARLA WILLIAMS FRANCIS D15121",
  "amount": 4690,
  "date": "2025-08-05",
  "Account_Type": "EVERYDAY CHECKING ...7034",
  "bankaccount_id": 18668
}
```

##### Why They Match
1. ✅ **Transaction Type**: Platform transaction has `Transaction_Type == 'returned'`
2. ✅ **Keyword**: Bank transaction name contains "individual" (from keywords list)
3. ✅ **Amount**: Both transactions are exactly $4,690
4. ✅ **Date**: Both transactions occurred on 2025-08-05
5. ✅ **Positive Amount**: Bank transaction amount > 0
6. ✅ **Bank Account ID**: Platform transaction's `from_account.bankaccount_id` matches bank transaction's `bankaccount_id`
7. ✅ **Bank Account ID Match**: 18668 = 18668 ✓

## 3. Key Insights

### 3.1 Received Transaction Insights

#### Platform Transactions
- **Identification**: Filtered by `Transaction_Type == 'received'`
- **Purpose**: These represent funds coming into the platform from external sources
- **Account Types**: Often involve "Checkbook Custodial" or "External account" accounts

#### Bank Transactions
- **Identification**: Filtered by negative amounts (`amount < 0`)
- **Purpose**: These represent the actual bank movements where funds are being received
- **Date Matching**: Exact date matches are prioritized over closest date matches

#### Checkbook Payments
- **Identification**: Filtered by recipient, direction, description, and amount
- **Purpose**: These validate that the transaction has a corresponding checkbook payment
- **Criteria**: Must be OUTGOING, contain "fund" or start with "F", and match the user_id

### 3.2 Returned Transaction Insights

#### Platform Transactions
- **Identification**: Filtered by `Transaction_Type == 'returned'`
- **Purpose**: These represent funds being returned to players
- **Account Types**: Often involve "Checkbook Custodial" or "Backer bank account" accounts

#### Bank Transactions
- **Identification**: Filtered by keywords in transaction names
- **Keywords Used**: 
  - `"checkbook"` - Found in account names like "Checkbook Custodial"
  - `"individual"` - Found in transaction names like "D15121 INDIVIDUAL..."
  - `"reel ventures"` - For Reel Ventures related transactions
  - `"rv enhanced wall"` - For RV Enhanced Wall related transactions
- **Purpose**: These represent the actual bank movements that correspond to returned funds

#### Matching Success Rate
- **100% Match Rate**: When all returned platform transactions find corresponding bank transactions
- **Perfect Alignment**: Amount, date, keyword, and bank account ID criteria all align
- **No Unmatched**: All transactions are successfully paired

#### Bank Account ID Matching
- **Critical Criterion**: Platform transaction's `from_account.bankaccount_id` must match bank transaction's `bankaccount_id`
- **Purpose**: Ensures funds are being returned from the correct bank account
- **Example**: Platform transaction `from_account.bankaccount_id: 18668` matches bank transaction `bankaccount_id: 18668`
- **Validation**: Prevents mismatched transactions between different bank accounts

## 4. Algorithm Benefits

### 4.1 Received Transaction Algorithm Benefits
1. **Date Prioritization**: Prioritizes exact date matches over closest date matches
2. **Checkbook Validation**: Ensures transactions have corresponding checkbook payments
3. **Precise Date Matching**: Uses ±2 days tolerance for better accuracy
4. **High Accuracy**: Correctly matches transactions to intended bank transactions
5. **Scalable**: Can handle large volumes of transactions efficiently

### 4.2 Returned Transaction Algorithm Benefits
1. **Precise Filtering**: Uses exact transaction type for platform transactions
2. **Keyword-Based Matching**: Flexible keyword system for bank transaction identification
3. **Multi-Criteria Matching**: Combines amount, date, and keyword validation
4. **High Accuracy**: Results in 100% match rates when criteria align
5. **Scalable**: Can handle large volumes of transactions efficiently

## 5. Configuration

### 5.1 Received Transaction Algorithm Configuration
The algorithm uses the following criteria:
- **Date Tolerance**: ±2 days for bank transaction matching
- **Checkbook Payment Criteria**: 
  - `recipient` must match `user_id`
  - `direction` must be `'OUTGOING'`
  - `description` must contain "fund" (case-insensitive) OR start with "F"
- **Amount Tolerance**: $0.01 for amount matching

### 5.2 Returned Transaction Algorithm Configuration
The algorithm is configured through `config.py`:
```python
RETURNED_KEYWORDS = ["checkbook", "reel ventures", "rv enhanced wall", "individual"]
```

## 6. Usage

### 6.1 Received Transaction Algorithm Usage
```python
from received_transaction_matcher import SimpleReceivedTransactionMatcher

# Create matcher with transactions and user_id
matcher = SimpleReceivedTransactionMatcher(
    platform_transactions=platform_data,
    bank_transactions=bank_data,
    user_id=15121
)

# Perform matching
results = matcher.match_received_transactions()

# Access results
print(f"Total Matches: {results.summary.total_matches}")
print(f"Match Rate: {results.summary.match_rate:.1f}%")
```

### 6.2 Returned Transaction Algorithm Usage
```python
from returned_transaction_matcher import OptimizedTransactionMatcher

# Create matcher with transactions and keywords
matcher = OptimizedTransactionMatcher(
    platform_transactions=platform_data,
    bank_transactions=bank_data,
    returned_keywords=RETURNED_KEYWORDS
)

# Perform matching
results = matcher.match_returned_transactions()

# Access results
print(f"Total Matches: {results.summary.total_matches}")
print(f"Match Rate: {results.summary.match_rate:.2f}%")
```

## 7. Summary

### 7.1 Received Transaction Algorithm Summary
The **Received Transaction Algorithm** successfully matches platform transactions where funds are coming INTO the platform by:
1. **Filtering platform transactions** by `Transaction_Type == 'received'`
2. **Filtering bank transactions** by negative amounts (`amount < 0`)
3. **Validating checkbook payments** with specific criteria (recipient, direction, description)
4. **Matching transactions** based on amount, date, bank account ID, and checkbook payment existence
5. **Prioritizing exact date matches** over closest date matches

This approach ensures accurate reconciliation between platform deposits and actual bank movements, correctly matching transactions to their intended bank transactions.

### 7.2 Returned Transaction Algorithm Summary
The **Returned Transaction Algorithm** successfully matches platform transactions where funds are being returned FROM the platform by:
1. **Filtering platform transactions** by `Transaction_Type == 'returned'`
2. **Filtering bank transactions** by keywords and positive amounts
3. **Matching transactions** based on amount, date, keyword, and bank account ID criteria

This approach ensures accurate reconciliation between platform returns and actual bank movements, achieving 100% match rates when all criteria align.

### 7.3 Combined Benefits
Both algorithms work together to provide comprehensive transaction matching:
- **Received Algorithm**: Handles funds coming INTO the platform with checkbook validation
- **Returned Algorithm**: Handles funds going OUT FROM the platform with keyword validation
- **High Accuracy**: Both algorithms achieve high match rates when criteria align
- **Scalable**: Can handle large volumes of transactions efficiently
