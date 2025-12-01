# Algorithm: Matching Deposit Transactions

## 1. Objective

This algorithm details the process of automatically matching "Deposit" type platform transactions with their corresponding bank transactions. The logic is designed to be robust, handling different withdrawal directions and ensuring a deterministic outcome when multiple potential matches are found.

## 2. Data Sources

- **Primary Input**: All platform and bank transactions for a given player, fetched via API.

- **Validation Source**: A master list of casino and vendor keywords stored in `CASINO_KEYWORDS.json`.

- **Output**: An update to the bank transaction record to link it to the corresponding platform transaction.

## 3. Data Preparation and Filtering

The process is executed on a per-player basis.

1.  **Fetch Platform Transactions**: For a given player, fetch all platform transactions. Filter this list to retain only transactions where:

    - `Transaction_Type` is 'deposit'.

    - The transaction is not already linked (i.e., `related_bank_transaction` is empty).

2.  **Early Exit**: If no processable platform withdrawals are found the process for this player stops here to avoid unnecessary API calls.

4.  **Fetch Bank Transactions**: Fetch all of the player's bank transactions. Filter this list to retain only transactions where:

    - The transaction is not already linked (i.e., `linked_transaction` is `null`).

5.  **Sort for Determinism**: To ensure a consistent and predictable matching order, sort both the filtered platform transactions and bank transactions by their respective dates in ascending (chronological) order.

## 4. Matching Logic

The core logic iterates through each prepared platform transaction and searches for a corresponding bank transaction.

### A. Pre-computation

For each platform transaction (`pt`):

1.  Round the `Amount` to two decimal places.

2.  Parse the `Date` string into a datetime object.

3.  Identify the `from` account's `bankaccount_id`. If this ID is missing, the transaction cannot be matched and is skipped.

For each bank transaction (`bt`) being compared:

1.  Round the `amount` to two decimal places and must be a positive amount.

2.  Parse the `date` string into a datetime object.

3.  Get the `bankaccount_id`.

4.  Create a standardized `description` string by concatenating the `name` and `counterparty_name` fields and converting to uppercase.

### B. Matching Rules

A bank transaction is considered a valid match for a platform transaction if **all** of the following criteria are met:

1.  **Amount Match**: The rounded bank transaction amount is an **exact positive match** for the rounded platform transaction amount (`bt.amount == pt.Amount`).

2.  **Date Proximity**: The bank transaction date is within a **9-day window** (plus or minus) of the platform transaction date.

3.  **Account Match**: The `bankaccount_id` of the bank transaction is the same as the `from` account's `bankaccount_id` from the platform transaction.

4.  **Dynamic Keyword Validation**:

    a. Get the casino name from the platform transaction's `Name` field.

    b. Look up the list of associated keywords for this casino name from the loaded `CASINO_KEYWORDS.json` data.

    c. The standardized `description` of the bank transaction must contain at least one of these specific keywords.

## 5. Linking and Finalization

The script uses a deterministic approach to handle cases with one or more potential matches.

1.  **Find First Match**: For each platform transaction, iterate through the chronologically sorted list of bank transactions.

2.  **Select and Link**: The **very first** bank transaction that satisfies all the rules in Section 4.B is considered the correct match.

3.  **Stop Searching**: Once this first match is found, the script immediately stops searching for other matches for the current platform transaction and proceeds to the next one.

4.  **Prevent Re-matching**: The matched bank transaction is removed from the pool of available bank transactions for the current player, ensuring it cannot be matched again.

5.  **No Match**: If the script iterates through all available bank transactions and finds none that meet the criteria, the platform transaction remains unmatched for a future run.

## 6. Dry Run Mode

The script must include a `--dry-run` flag. When enabled:

- No actual linking operations are performed.

- Instead, the script will log detailed information about the matches it identifies, providing a clear report of the actions it would have taken.

