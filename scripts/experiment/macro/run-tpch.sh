#!/bin/bash

# Input parameters
database_path=$1
queries_path=$2
query_numbers=$3
output_file=$4

# Function to parse query numbers
parse_query_numbers() {
    IFS=';' read -ra PARTS <<< "$1"
    for part in "${PARTS[@]}"; do
        if [[ $part == *-* ]]; then
            IFS='-' read -ra RANGE <<< "$part"
            for i in $(seq "${RANGE[0]}" "${RANGE[1]}"); do
                echo "$i"
            done
        else
            echo "$part"
        fi
    done
}

# Parse query numbers
queries=$(parse_query_numbers $query_numbers)

# Prepare the output file
echo "Query number,Compile time,Query execution time" > $output_file

# Run Store
../../build-release/tbgpp-graph-store/store &
sleep 5

# Execute queries
for query_num in $queries; do
    query_file="${queries_path}/q${query_num}.cql"
    if [ ! -f "$query_file" ]; then
        echo "Query file $query_file not found!"
        continue
    fi
    query_str=$(cat "$query_file")
    warmup_output=$(timeout 3600s ../../build-release/tbgpp-client/TurboGraph-S62 --workspace:${database_path} --query:"$query_str" --disable-merge-join --join-order-optimizer:exhaustive)
    output_str=$(timeout 3600s ../../build-release/tbgpp-client/TurboGraph-S62 --workspace:${database_path} --query:"$query_str" --disable-merge-join --num-iterations:3 --join-order-optimizer:exhaustive)
    exit_status=$?

    if [ $exit_status -ne 0 ]; then
        echo "$query_num, timeout, timeout" >> $output_file
        continue
    fi

    # Extract times
    compile_time=$(echo "$output_str" | grep -oP 'Average Compile Time: \K[0-9.]+')
    exec_time=$(echo "$output_str" | grep -oP 'Average Query Execution Time: \K[0-9.]+')

    # Write to output file
    echo "$query_num, $compile_time, $exec_time" >> "$output_file"
done

pkill -f store
sleep 3
