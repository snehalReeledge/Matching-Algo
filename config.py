#!/usr/bin/env python3
"""
Configuration file for the Received/Returned Transaction Matching Algorithm
Contains all constants, settings, and configuration values.
"""

# API Configuration
PLAYERS_API_URL = "https://xhks-nxia-vlqr.n7c.xano.io/api:G9pW0Uty/players"
BANK_TRANSACTIONS_API_URL = "https://xhks-nxia-vlqr.n7c.xano.io/api:1ZwRS-f0/getUserBankTransactions"
PLATFORM_TRANSACTIONS_API_URL = "https://xhks-nxia-vlqr.n7c.xano.io/api:1ZwRS-f0/getTransactions"
CHECKBOOK_PAYMENTS_API_URL = "https://xhks-nxia-vlqr.n7c.xano.io/api:1ZwRS-f0/getCheckbookPayments"
UPDATE_BANK_TRANSACTIONS_API_URL = "https://xhks-nxia-vlqr.n7c.xano.io/api:1ZwRS-f0/updateBankTransaction/{transaction_id}"
UPDATE_PLATFORM_TRANSACTIONS_API_URL = "https://xhks-nxia-vlqr.n7c.xano.io/api:1ZwRS-f0/transactions/{platform_transaction_id}"
CREATE_PLATFORM_TRANSACTION_API_URL = "https://xhks-nxia-vlqr.n7c.xano.io/api:1ZwRS-f0/transactions" #558
SCRAPED_TRANSACTION_API_URL = "https://xhks-nxia-vlqr.n7c.xano.io/api:8LbPEFvA/getUserScrapedTransactions"
USER_ACCOUNTS_API_URL = "https://xhks-nxia-vlqr.n7c.xano.io/api:1ZwRS-f0/getUserAccounts" #561
DELETE_PLATFORM_TRANSACTIONS_API_URL = "https://xhks-nxia-vlqr.n7c.xano.io/api:1ZwRS-f0/transactions"
CASINOACCOUNTS_API_URL = "https://xhks-nxia-vlqr.n7c.xano.io/api:1ZwRS-f0/getCasinoAccounts"
UPDATE_CASINO_ACCOUNTS_API_URL = "https://xhks-nxia-vlqr.n7c.xano.io/api:clWeqHId/updatePlayPlusCasinoAccounts"
UPDATE_SCRAPED_TRANSACTIONS_API_URL = "https://xhks-nxia-vlqr.n7c.xano.io/api:clWeqHId/scrapedtransactions/{id}"

# API Settings
API_TIMEOUT = 30  # API timeout in seconds

# Keywords for matching returned transactions
RETURNED_KEYWORDS = ["checkbook", "reel ventures", "rv enhanced wall", "individual"]

# Logging Configuration
LOG_LEVEL = "WARNING"  # Changed from INFO to WARNING for performance

# Matcher Configuration
TRANSFER_ACCOUNT_ID = 36715
FEES_ACCOUNT_ID = 18
AI_USER_ID = 35047

