# Algorithm: PayPal Withdrawal Reconciliation Matcher

## 1. Objective

This algorithm runs *after* the main `withdrawal_matcher` and serves as a second pass to find and correct miscategorized withdrawal transactions. It identifies unmatched platform withdrawals that, based on scraped PayPal data, should have been directed to a user's Betting PayPal account. It then finds the corresponding bank transaction, links it, and updates the platform transaction's destination account.

## 2. Data Sources

- **Platform Transactions**: Unmatched transactions of type 'Withdrawal'.
- **Scraped Transactions**: Scraped data fetched via `SCRAPED_TRANSACTION_API_URL`.
- **Bank Transactions**: Unmatched transactions from the user's default Betting PayPal account.
- **User Accounts**: Fetched via `USER_ACCOUNTS_API_URL` to identify the default Betting PayPal account.
- **Casino Keywords**: The `CASINO_KEYWORDS.json` file, used to look up keywords based on a casino name.

## 3. Algorithm Steps

The process is executed on a per-player basis.

### Step 1: Data Fetching and Preparation

1.  **Get Players**: Retrieve the list of players to process.
2.  **Get Unmatched Withdrawals**: For each player, fetch all platform transactions and filter for those where `Transaction_Type` is 'withdrawal' and `related_bank_transaction` is empty.
3.  **Apply 15-Day Holdback**: Further filter the list of unmatched withdrawals, removing any transactions with a `Date` that is within the last 15 days. This prevents the script from processing transactions that may still be in transit.
4.  **Get Scraped Transactions**: Fetch all of the player's scraped transactions. Filter for those where `Source` is 'paypal' and `Type` is 'transfer_received'.
5.  **Identify Default PayPal Account**: Fetch the player's user accounts and identify the one where `Account_Type` is 'Betting PayPal account' and `isDefault` is `true`. Store its `id` (the platform account ID) and `bankaccount_id`. If none is found, the player cannot be processed.
6.  **Get PayPal Bank Transactions**: Fetch all of the player's bank transactions. Filter for those that are unmatched (`linked_transaction` is `null`) and belong to the default PayPal account (matching the `bankaccount_id` from the previous step).
7.  **Load Casino Keywords**: Load the `CASINO_KEYWORDS.json` file into a structure that allows looking up a list of keywords by a casino name (e.g., a dictionary where the key is the casino name).

### Step 2: Primary Match (Platform to Scraped)

Iterate through each unmatched platform withdrawal (`pt`). For each one, search through the filtered scraped transactions (`st`) to find a match.

A scraped transaction is a potential match if it meets **all** of the following criteria:
1.  **Amount Match**: `abs(pt.Amount - st.amount) < 0.01` (using a small tolerance for floating point safety).
2.  **Date Proximity**: The date of the scraped transaction is within a **7-day window** (plus or minus) of the platform transaction's date.

If no matching scraped transaction is found for the platform transaction, move to the next platform transaction.

### Step 3: Secondary Match (Platform to Bank)

If a matching scraped transaction is found, proceed to find the corresponding bank transaction (`bt`) from the pre-filtered list of PayPal bank transactions.

A bank transaction is a definitive match if it meets **all** of the following criteria:

1.  **Amount Match**: The bank transaction amount is an **exact negative match** for the platform transaction amount, after rounding both to two decimal places (`round(bt.amount, 2) == -round(pt.Amount, 2)`).
2.  **Date Proximity**: The bank transaction date is within a **9-day window** (plus or minus) of the platform transaction date.
3.  **Dynamic Keyword Validation**:
    a. Get the casino name from the platform transaction's `Name` field.
    b. Look up the list of associated keywords for this casino name from the loaded `CASINO_KEYWORDS.json` data.
    c. The bank transaction's description (`name` or `counterparty_name`) must contain at least one of these specific keywords.

### Step 4: Action and Finalization

If a single, unique bank transaction is found that meets all the secondary match criteria:

1.  **Update Platform Transaction**: Send a `PATCH` request to update the platform transaction. The payload will set the `To_Account` field to the `id` of the user's default Betting PayPal account.
2.  **Link Bank Transaction**: Send a `PATCH` request to update the bank transaction, setting its `transaction_link` field to the `id` of the platform transaction.

If multiple bank transactions are found, the script should follow a deterministic rule (e.g., pick the one closest in date, or the first one found in the chronologically sorted list) but flag the choice with a warning.

## 5. Dry Run Mode

The script must include a `--dry-run` flag. When enabled:
- No `PATCH` requests will be made.
- The script will log detailed information about the matches it identifies and the actions (update and link) it would have performed.
