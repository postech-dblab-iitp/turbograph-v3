#!/bin/bash

# Function to parse the average query execution time from a file
parse_time() {
    grep "Average Compile Time" "$1" | head -n 1 | sed 's/.*Average Compile Time: \([0-9.]*\) ms/\1/'
}

# Input folder
input_folder=$1

# Initialize an associative array to store the times
declare -A times

DIST=OPTICS

# Read files in the input folder
for file in "$input_folder"/goodbye_zipf_*_Q*_${DIST}_OURS_ASCENDING.txt; do
    # Extract distribution and query number from the filename
    if [[ $file =~ goodbye_zipf_([0-9]+)_Q([0-9]+)_${DIST}_OURS_ASCENDING.txt ]]; then
        distribution=${BASH_REMATCH[1]}
        query_num=${BASH_REMATCH[2]}
        
        # Parse the time from the file
        time=$(parse_time "$file")

        # Store the time in the associative array
        times["$distribution,$query_num"]=$time
    fi
done

# Get unique query numbers and distributions
unique_queries=$(for key in "${!times[@]}"; do echo "${key#*,}"; done | sort -n | uniq)
unique_distributions=$(for key in "${!times[@]}"; do echo "${key%,*}"; done | sort -n | uniq)

# Output the CSV header
echo -n "Distribution"
for query_num in $unique_queries; do
    echo -n ",Q$query_num"
done
echo

# Output the CSV rows
for distribution in $unique_distributions; do
    echo -n "$distribution"
    for query_num in $unique_queries; do
        key="$distribution,$query_num"
        if [[ -v times[$key] ]]; then
            echo -n ",${times[$key]}"
        else
            echo -n ",-"
        fi
    done
    echo
done
