#!/bin/bash

# Define paths and credentials
REMOTE_USER="jhha"
REMOTE_HOST="141.223.197.35"
REMOTE_PATH="/mnt/sdc/turbograph-s62/s62-benchmark-runner/data-preprocessor/kg/s62/query-gen/out"
LOCAL_PATH="/turbograph-v3/queries/kg/yago-incremental"
SCRIPT_PATH="/turbograph-v3/scripts/experiment/clustering/kg/yago/run-yago-incremental.sh"
PASSWORD="5ba1149b17!"

# Keep track of previously copied files
COPIED_FILES="$LOCAL_PATH/copied_files.txt"
mkdir -p "$LOCAL_PATH"
touch "$COPIED_FILES"

# Function to copy new files and run the script
sync_and_run() {
  echo "Starting file copy..."
  # Copy new files from remote to local using sshpass with scp
  sshpass -p "$PASSWORD" scp -r "$REMOTE_USER@$REMOTE_HOST:$REMOTE_PATH/"* "$LOCAL_PATH/" 2>&1 | tee -a copy_debug.log

  # Check if scp encountered an error
  if [[ $? -ne 0 ]]; then
    echo "Error during file copy. Check copy_debug.log for details."
    return
  fi

  # Get the list of files in the local directory
  CURRENT_FILES=$(ls "$LOCAL_PATH"/*.cql 2>/dev/null)

  # Check if any new .cql files are found
  if [[ -z "$CURRENT_FILES" ]]; then
    echo "No .cql files found in local directory."
    return
  fi

  # Find new files by comparing current list with previously copied files
  NEW_FILES=$(comm -23 <(echo "$CURRENT_FILES" | xargs -n 1 basename | sort) <(sort "$COPIED_FILES"))

  # Proceed if there are new files
  if [[ -n "$NEW_FILES" ]]; then
    echo "$NEW_FILES" >> "$COPIED_FILES"
    
    # Extract base names without extensions and join them with ";"
    QUERY_NAMES=$(echo "$NEW_FILES" | sed 's/\.cql$//' | paste -sd';' -)
    echo "Running clustering script with queries: $QUERY_NAMES"
    
    # Run the clustering script with new queries
    bash "$SCRIPT_PATH" "$QUERY_NAMES"
  else
    echo "No new files found."
  fi
}

# Infinite loop to repeat every 10 minutes
while true; do
  sync_and_run
  sleep 10  # Wait 10 minutes before the next sync
done
