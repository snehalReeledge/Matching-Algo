import argparse
from create_fee_transactions import FeeTransactionCreator

def main():
    """
    Main function to initialize and run the fee transaction creation process.
    Handles command-line arguments for different run modes.
    """
    parser = argparse.ArgumentParser(description="Create and link fee platform transactions based on unmatched bank transactions.")
    parser.add_argument('--dry-run', action='store_true', help="Run in report-only mode without making any actual data changes.")
    parser.add_argument('--player-id', type=int, help="Run for a single player ID.")
    parser.add_argument('--player-stage', type=str, help="Run for all players in a specific stage.")
    
    args = parser.parse_args()

    # Initialize the creator and run the process
    creator = FeeTransactionCreator()
    creator.process_players(dry_run=args.dry_run, player_id=args.player_id, player_stage=args.player_stage)

if __name__ == "__main__":
    main()
