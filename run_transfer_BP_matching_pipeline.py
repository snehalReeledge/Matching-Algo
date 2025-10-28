import subprocess
import sys

def run_command(command):
    """Runs a command, prints its output in real-time, and checks for errors."""
    print(f"Running command: {' '.join(command)}")
    try:
        # Using Popen to stream output in real-time
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding='utf-8', bufsize=1)
        
        for line in iter(process.stdout.readline, ''):
            print(line, end='')
        
        process.stdout.close()
        return_code = process.wait()

        if return_code:
            print(f"\nError running command: {' '.join(command)}")
            print(f"Return code: {return_code}")
            sys.exit(return_code)
        
        print(f"Command completed successfully: {' '.join(command)}\n")

    except FileNotFoundError:
        print(f"Error: The command '{command[0]}' was not found.")
        print("Please ensure Python is in your PATH and you are in the correct directory.")
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python run_transfer_BP_matching_pipeline.py <player_id> [other_args...]")
        sys.exit(1)

    player_id = sys.argv[1]
    other_args = sys.argv[2:]

    print("--- Starting Matching and Linking Pipeline ---")

    # Command 1: Run simplified matcher
    command1 = ["python", "run_simplified_matcher.py", player_id] + other_args
    run_command(command1)

    # Command 2: Create and link unmatched transfers
    command2 = ["python", "create_and_link_unmatched_transfers.py", player_id] + other_args
    run_command(command2)

    print("--- Matching and Linking Pipeline Completed Successfully ---")
