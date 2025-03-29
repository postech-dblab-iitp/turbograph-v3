#!/bin/bash

# Input parameters
database_path=$1
queries_path=$2
query_numbers=$3
output_file=$4

log_dir_base="/turbograph-v3/logs"
current_datetime=$(date +"%Y-%m-%d")
log_dir="${log_dir_base}/query/${current_datetime}"
mkdir -p ${log_dir}

# Function to parse query numbers
parse_query_numbers() {
    if [[ $1 == *-* ]]; then
        IFS='-' read -ra RANGE <<< "$1"
        for i in $(seq ${RANGE[0]} ${RANGE[1]}); do
            echo $i
        done
    elif [[ $1 == *';'* ]]; then
        IFS=';' read -ra NUMS <<< "$1"
        for i in "${NUMS[@]}"; do
            echo $i
        done
    else
        echo $1
    fi
}

# Parse query numbers
queries=$(parse_query_numbers $query_numbers)

# Prepare the output file
echo "Query number,Compile time,Query execution time" > $output_file

# Execute queries
for query_num in $queries; do
    query_file="${queries_path}/q${query_num}.cql"
    if [ ! -f "$query_file" ]; then
        echo "Query file $query_file not found!"
        continue
    fi
    log_file="${log_dir}/q${query_num}.txt"
    query_str=$(cat "$query_file")

    # Run query
    output_str=$(timeout 3600s \
        /turbograph-v3/build-release/tools/client \
        --standalone \
        --slient \
        --workspace ${database_path} \
        --query "${query_str}" \
        --disable-merge-join \
        --iterations 3 \
        --join-order-optimizer exhaustive \
        --warmup)

    # Output to log file
    echo "$output_str" >> "$log_file"

    # Extract times
    compile_time=$(echo "$output_str" | grep -oP 'Average Compile Time: \K[0-9.]+' | awk '{printf "%.2f", $1}')
    exec_time=$(echo "$output_str" | grep -oP 'Average Query Execution Time: \K[0-9.]+' | awk '{printf "%.2f", $1}')
    end_to_end_time=$(echo "$output_str" | grep -oP 'Average End to End Time: \K[0-9.]+' | awk '{printf "%.2f", $1}')

    echo "Q$query_num, $compile_time, $exec_time, $end_to_end_time" >> "$output_file"
done