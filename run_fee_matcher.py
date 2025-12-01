"""
Script to run the Fee Matcher.

This script imports the matching logic from fee_matcher.py and runs it.
By default, it performs a dry run, printing potential matches for review.
To execute a live run that links transactions, use the --live flag.
Example: python run_fee_matcher.py --live
"""
import argparse
import requests
import sys
from fee_matcher import match_fee_transactions_for_player
from config import PLAYERS_API_URL


def fetch_player_ids_by_stages(stages):
    """Fetches player IDs for the given stages."""
    player_ids = set()
    for stage in stages:
        print(f"Fetching players for stage: {stage}...")
        try:
            response = requests.get(PLAYERS_API_URL, params={'player_stage': stage}, timeout=60)
            response.raise_for_status()
            players = response.json()
            ids = {p['id'] for p in players if p.get('id')}
            print(f"Found {len(ids)} players in stage '{stage}'.")
            player_ids.update(ids)
        except requests.exceptions.RequestException as e:
            print(f"Error fetching players for stage '{stage}': {e}")
    return list(player_ids)

def fetch_all_player_ids():
    """Fetches all player IDs."""
    print("Fetching all players...")
    try:
        response = requests.get(PLAYERS_API_URL, timeout=60)
        response.raise_for_status()
        players = response.json()
        player_ids = {p['id'] for p in players if p.get('id')}
        print(f"Found {len(player_ids)} total players.")
        return list(player_ids)
    except requests.exceptions.RequestException as e:
        print(f"Fatal: Could not fetch players. {e}")
        return []


def main():
    """
    Main function to drive the matching process.
    Parses command-line arguments to determine which players to run on and in which mode.
    """
    parser = argparse.ArgumentParser(description="Run the fee matcher. Accepts one or more player IDs.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--player_ids', nargs='+', type=int, help='One or more player IDs to process.')
    group.add_argument('--player_stages', nargs='+', help="Run for players in specific stages (e.g., 'active' 'new').")
    group.add_argument('--all-stages', action='store_true', help="Run for all players, across all stages.")
    parser.add_argument('--live', action='store_true', help="Run in live mode, which will link transactions.")
    args = parser.parse_args()

    player_ids_to_process = []
    if args.player_ids:
        player_ids_to_process = args.player_ids
    elif args.player_stages:
        player_ids_to_process = fetch_player_ids_by_stages(args.player_stages)
    elif args.all_stages:
        player_ids_to_process = fetch_all_player_ids()

    if not player_ids_to_process:
        print("No players found to process. Exiting.")
        sys.exit(0)

    is_dry_run = not args.live
    run_mode = "DRY RUN" if is_dry_run else "LIVE RUN"

    print(f"\nStarting Fee Matcher in {run_mode} mode for {len(player_ids_to_process)} players.")

    total_matches = 0
    for player_id in player_ids_to_process:
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
