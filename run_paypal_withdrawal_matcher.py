import argparse
from paypal_withdrawal_matcher import PaypalWithdrawalMatcher

def main():
    """
    Main function to initialize and run the PayPal withdrawal reconciliation matcher.
    Handles command-line arguments for different run modes.
    """
    parser = argparse.ArgumentParser(
        description="Reconcile unmatched withdrawal platform transactions using PayPal scraped data."
    )
    parser.add_argument(
        '--dry-run', 
        action='store_true', 
        help="Run the matcher in report-only mode without making any actual data changes."
    )
    parser.add_argument(
        '--player-id', 
        type=int, 
        help="Run the matcher for a single player ID."
    )
    parser.add_argument(
        '--player-stages',
        nargs='+',
        type=str,
        help="Run the matcher for all players in one or more stages (e.g., 'Batch 1' 'Batch 2')."
    )

    args = parser.parse_args()

    # Initialize the matcher and run the process
    matcher = PaypalWithdrawalMatcher()
    matcher.run(dry_run=args.dry_run, player_id=args.player_id, player_stages=args.player_stages)

if __name__ == "__main__":
    main()
