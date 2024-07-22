#!/bin/bash

# Define the possible values for each configuration
num_schemas=("2")

# Define target and log directories
target_dir_base="/data/goodbye/coalesce/"
log_dir_base="/turbograph-v3/logs"

# Get current date and time for log directory
log_dir="${log_dir_base}/coalesce"

# Input parameters
queries_path="/turbograph-v3/queries/goodbye/sf1/zipf-0"
query_numbers="1;2;3;4;5;6;8"

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

# Loop over all combinations of cost models, distributions, and thresholds
for num_schema in "${num_schemas[@]}"; do
    target_dir="${target_dir_base}/schema-${num_schema}"
    for query_num in $queries; do
        query_file="${queries_path}/q${query_num}.cql"
        if [ ! -f "$query_file" ]; then
            echo "Query file $query_file not found!"
            continue
        fi
        query_str=$(cat "$query_file")
        echo "Running query $query_num with $num_schema schemas"
        echo "Query: $query_str"

        log_file="${log_dir}/without-coalescing-Q${query_num}-schema-${num_schema}.txt"

        # Run store
        /turbograph-v3/build-release/tbgpp-graph-store/store 200GB&
        sleep 5

        # Reun query
        timeout 3600s \
            /turbograph-v3/build-release/tbgpp-client/TurboGraph-S62 --workspace:${target_dir} --query:"$query_str" --disable-merge-join --num-iterations:4 --join-order-optimizer:exhaustive --warmup --compile-only \
            >> ${log_file}

        pkill -f store
        sleep 5
    done
done
