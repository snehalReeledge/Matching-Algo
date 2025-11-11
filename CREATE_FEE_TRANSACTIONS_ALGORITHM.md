# Algorithm: Create and Link Fee Platform Transactions from Bank Transactions

## 1. Objective

This algorithm details the process of reverse-engineering fee transactions. It identifies specific, unmatched bank transactions that represent fees, creates the corresponding "Fees" type platform transaction, and then links the two. This is designed to correct instances where a fee transaction exists in the bank data but is missing from the platform data.

## 2. Data Sources

- **Primary Input**: Unmatched bank transactions for each player, fetched via the `BANK_TRANSACTIONS_API_URL`.
- **Ancillary Input**: User account data, fetched via `USER_ACCOUNTS_API_URL`, to map a `bankaccount_id` to a user's platform account ID.
- **Output**: 
    - A new platform transaction created via a `POST` request to `CREATE_PLATFORM_TRANSACTIONS_API_URL`.
    - An update to the source bank transaction via a `PATCH` request to `UPDATE_BANK_TRANSACTIONS_API_URL` to establish the link.

## 3. Key Constants & Keywords

- **`FEES_ACCOUNT_ID`**: The static platform account ID representing "Fees".
- **`AI_USER_ID`**: The user ID to be logged as the creator of the new transactions.
- **Amount Threshold**: The absolute amount of the bank transaction must be greater than 0 and less than or equal to `2.00`.
- **Keyword Lists**: Two sets of keywords are used to identify potential fee transactions from the bank transaction's `name` or `counterparty_name` fields.
    - **`BETTING_BANK_TO_FEES_KEYWORDS`** (for positive amounts):
        - `Checkbook Inc MICRO DEP`
        - `CHECKBOOK INC ACCTVERIFY`
        - `PAYPAL ACCTVERIFY`
        - `SIGHTLINE_BNKGEO ACCOUNTREG`
        - `SIGHTLINE_SUTTON`
        - `HSAWCSPCUSTODIAN ACCTVERIFY`
    - **`FEES_TO_BETTING_BANK_KEYWORDS`** (for negative amounts):
        - `MONTHLY SERVICE FEE`
        - `OVERDRAFT`
        - `CHECKBOOK INC`
        - `PAYPAL ACCTVERIFY`
        - `SIGHTLINE_SUTTON`
        - `HSAWCSPCUSTODIAN ACCTVERIFY`

## 4. Algorithm Steps

The process is executed on a per-player basis.

### Step 1: Data Fetching

1.  Retrieve a list of all players to be processed.
2.  For each player, fetch all their bank transactions from the `BANK_TRANSACTIONS_API_URL`.
3.  Filter this list to retain only bank transactions where the `linked_transaction` field is `null`.
4.  Fetch all of the player's platform accounts using the `USER_ACCOUNTS_API_URL`. Create a mapping of `bankaccount_id` to the account's primary `id`. This is crucial for determining the `From_Account` or `To_Account`.

### Step 2: Transaction Analysis

For each unmatched bank transaction:

1.  **Amount Check**: Verify that `0 < abs(amount) <= 2.00`. If this condition is not met, skip to the next transaction.
2.  **Keyword Check**: Check if the bank transaction's `name` or `counterparty_name` contains any of the keywords from the two lists defined above. If no keyword is found, skip.

### Step 3: Direction Logic & Payload Creation

If a bank transaction passes the checks, determine the direction and prepare the payload for the new platform transaction.

**Case A: Amount is POSITIVE (`bt.amount > 0`)**

This signifies a credit to the user's bank account (e.g., a micro-deposit for verification). This corresponds to a platform transaction from the **Betting Bank to Fees**.

- **Keyword Validation**: The description must match a keyword from the `BETTING_BANK_TO_FEES_KEYWORDS` list.
- **Payload Construction**:
    - `From_Account`: The user's platform account `id` corresponding to the `bt.bankaccount_id` (retrieved from the mapping in Step 1).
    - `To_Account`: `FEES_ACCOUNT_ID`.
    - `Transaction_Type`: `"fees"`
    - `Amount`: `bt.amount`
    - `Date`: `bt.date`
    - `User_ID`: `player_id`
    - `Added_By`: `AI_USER_ID`
    - `Status`: `"Completed"`

**Case B: Amount is NEGATIVE (`bt.amount < 0`)**

This signifies a debit from the user's bank account (e.g., a monthly service charge). This corresponds to a platform transaction from **Fees to the Betting Bank**.

- **Keyword Validation**: The description must match a keyword from the `FEES_TO_BETTING_BANK_KEYWORDS` list.
- **Payload Construction**:
    - `From_Account`: `FEES_ACCOUNT_ID`.
    - `To_Account`: The user's platform account `id` corresponding to the `bt.bankaccount_id`.
    - `Transaction_Type`: `"fees"`
    - `Amount`: `abs(bt.amount)`
    - `Date`: `bt.date`
    - `User_ID`: `player_id`
    - `Added_By`: `AI_USER_ID`
    - `Status`: `"Completed"`

If the corresponding user account `id` cannot be found from the `bankaccount_id`, log an error and skip.

### Step 4: Transaction Creation and Linking

1.  **Create**: Send the constructed payload in a `POST` request to the `CREATE_PLATFORM_TRANSACTIONS_API_URL`.
2.  **Verify**: On successful creation, the API will return the newly created platform transaction, including its `id`.
3.  **Link**: Use the new platform transaction `id` to update the original bank transaction. Send a `PATCH` request to `UPDATE_BANK_TRANSACTIONS_API_URL` with a payload linking the two.

## 5. Dry Run Mode

The script must include a `--dry-run` flag. When enabled:
- No `POST` or `PATCH` requests will be made.
- The script will log detailed information about the bank transactions it identifies and the platform transactions it *would* have created and linked.
