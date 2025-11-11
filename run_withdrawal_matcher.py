import argparse
from withdrawal_matcher import WithdrawalMatcher

def main():
    """
    Main function to initialize and run the withdrawal matching process.
    Handles command-line arguments for different run modes.
    """
    parser = argparse.ArgumentParser(description="Match platform withdrawal transactions with bank transactions.")
    parser.add_argument('--dry-run', action='store_true', help="Run the matcher in report-only mode without making any actual data changes.")
    parser.add_argument('--player-id', type=int, help="Run the matcher for a single player ID.")
    parser.add_argument('--player-stages', nargs='+', type=str, help="Run for players in one or more stages (e.g., 'Batch 1' 'Batch 2').")
    parser.add_argument('--limit', type=int, help="Limit the number of players to process from the selected stage.")
    
    args = parser.parse_args()

    # Initialize the matcher and run the process
    matcher = WithdrawalMatcher()
    matcher.find_matches(dry_run=args.dry_run, player_id=args.player_id, player_stages=args.player_stages, limit=args.limit)

if __name__ == "__main__":
    main()