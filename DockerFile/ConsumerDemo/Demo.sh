#!/bin/bash

# Define the directories that contain the Python files
directories=("Audio" "Image" "Store" "Video")

# Array to store the PIDs of the Python scripts
pids=()

# Iterate over the directories
for dir in "${directories[@]}"; do
    # Change to the directory
    cd "$dir"

    # Find all Python files and run them
    for file in *.py; do
        echo "Running $file in $dir"
        python3 "$file" &
        
        # Store the PID of the Python script
        pids+=($!)
    done

    # Change back to the parent directory
    cd -
done

# Wait for a while (optional)
sleep 10

# Kill the Python scripts
for pid in "${pids[@]}"; do
    kill -TERM "$pid"
done