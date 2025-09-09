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
UPDATE_BANK_TRANSACTIONS_API_URL = "https://xhks-nxia-vlqr.n7c.xano.io/api:VG8fVqug/banktransactions/{banktransactions_id}"

# API Settings
API_TIMEOUT = 30  # API timeout in seconds

# Returned transaction keywords
RETURNED_KEYWORDS = ["checkbook", "reel ventures", "rv enhanced wall", "individual"]

# Logging Configuration
LOG_LEVEL = "WARNING"  # Changed from INFO to WARNING for performance
