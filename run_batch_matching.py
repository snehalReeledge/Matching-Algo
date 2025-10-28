#!/usr/bin/env python3
"""
Run the PayPal to Bank transfer matching algorithm for a batch of players
based on their player stage.
"""

import os
import requests
import argparse
from datetime import datetime

# Import the main processing function from the single-player script
from run_transfer_bettingpaypal_matcher import run_matching_for_player
from config import PLAYERS_API_URL

def get_player_ids_by_stage(stage: str) -> list[int]:
    """Fetches all players and filters them to get a list of IDs for a given player stage."""
    player_ids = []
    print("Fetching all players to filter by stage...")
    try:
        # Fetch all players, as the API might not support server-side filtering
        response = requests.get(PLAYERS_API_URL)
        response.raise_for_status()
        players = response.json()
        
        if not players:
            print("No players found from the API.")
            return []
            
        # Filter players by the specified stage
        for player in players:
            # Assuming the stage is stored in a field named 'player_stage'
            if player.get('player_stage') == stage:
                if 'id' in player:
                    player_ids.append(player['id'])
                elif 'player_id' in player:
                    player_ids.append(player['player_id'])
        
        if not player_ids:
            print(f"No players found for stage '{stage}' after filtering.")
        else:
            print(f"Found {len(player_ids)} players for stage '{stage}'.")
            
        return player_ids
        
    except requests.RequestException as e:
        print(f"Error fetching players: {e}")
        return []

def main(player_stage: str):
    """Main function to run the batch matching process."""
    print(f"🚀 Starting batch matching for player stage: '{player_stage}'")
    
    player_ids = get_player_ids_by_stage(player_stage)
    
    if not player_ids:
        print("Exiting.")
        return
        
    # Create a single output directory for the entire batch
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    batch_dir = f"batch_run_{player_stage.replace(' ', '_')}_{timestamp}"
    os.makedirs(batch_dir, exist_ok=True)
    print(f"📁 Batch output directory: {batch_dir}")
    
    # Process each player
    for i, player_id in enumerate(player_ids, 1):
        print(f"\n--- Processing player {i}/{len(player_ids)} (ID: {player_id}) ---")
        try:
            run_matching_for_player(player_id, base_output_dir=batch_dir)
        except Exception as e:
            print(f"  ❌ An unexpected error occurred while processing player {player_id}: {e}")
            # Continue to the next player
            continue
            
        # Check for a stop file to allow for graceful shutdown
        if os.path.exists('stop.txt'):
            print("\n'stop.txt' file found. Stopping batch process after the current player.")
            os.remove('stop.txt')  # Clean up the stop file
            break  # Exit the loop

    print("\n🎉 Batch matching complete!")
    print(f"📁 All results saved to: {batch_dir}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run matching for a batch of players by their stage.")
    parser.add_argument("player_stage", type=str, help="The player stage to process (e.g., 'Batch 4').")
    args = parser.parse_args()
    
    main(args.player_stage)
