"""
Script to run the Fee Matcher.

This script imports the matching logic from fee_matcher.py and runs it.
By default, it performs a dry run, printing potential matches for review.
To execute a live run that links transactions, use the --live flag.
Example: python run_fee_matcher.py --live
"""
import argparse
from fee_matcher import match_fee_transactions_for_player


def main():
    """
    Main function to drive the matching process.
    Parses command-line arguments to determine which players to run on and in which mode.
    """
    parser = argparse.ArgumentParser(description="Run the fee matcher. Accepts one or more player IDs.")
    parser.add_argument('player_ids', nargs='+', type=int, help='One or more player IDs to process.')
    parser.add_argument('--live', action='store_true', help="Run in live mode, which will link transactions.")
    args = parser.parse_args()

    is_dry_run = not args.live
    run_mode = "DRY RUN" if is_dry_run else "LIVE RUN"

    print(f"Starting Fee Matcher in {run_mode} mode.")

    total_matches = 0
    for player_id in args.player_ids:
        print(f"\n--- Starting fee matching for player {player_id} ---")
        match_count = match_fee_transactions_for_player(player_id, dry_run=is_dry_run)
        
        if is_dry_run:
            print(f"Dry run complete for player {player_id}. Found {match_count} potential matches.")
        else:
            print(f"Live run complete for player {player_id}. Linked {match_count} transactions.")
        
        total_matches += match_count
    
    if is_dry_run:
        print(f"\nTotal potential matches found across all players: {total_matches}")
    else:
        print(f"\nTotal transactions linked across all players: {total_matches}")


if __name__ == "__main__":
    main()
