#!/bin/bash

# Define the possible values for each configuration
cluster_algorithms=("AGGLOMERATIVE")
cost_models=("OURS")
layering_orders=("DESCENDING")

# Define target and log directories
target_dir_base="/data/dbpedia/"
log_dir_base="/turbograph-v3/logs"

# Get current date and time for log directory
current_datetime=$(date +"%Y-%m-%d")
log_dir="${log_dir_base}/query/${current_datetime}"
mkdir -p ${log_dir}

# Input parameters
queries_path="/turbograph-v3/queries/dbpedia"
query_numbers="1-20"

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
for cluster_algo in "${cluster_algorithms[@]}"; do
    for cost_model in "${cost_models[@]}"; do
        for layering_order in "${layering_orders[@]}"; do
            for query_num in $queries; do
                query_file="${queries_path}/q${query_num}.cql"
                if [ ! -f "$query_file" ]; then
                    echo "Query file $query_file not found!"
                    continue
                fi

                # Setup
                target_dir="${target_dir_base}/dbpedia_${cluster_algo}_${cost_model}_${layering_order}"
                log_file="${log_dir}/dbpedia_Q${query_num}_${cluster_algo}_${cost_model}_${layering_order}.txt"

                # Run store
                /turbograph-v3/build-release/s62-graph-store/store 365GB&
                sleep 15

                # Run query
                timeout 3600s \
                    /turbograph-v3/build/s62-client/s62_cli --workspace:${target_dir} --query-file:${query_file} --disable-merge-join --num-iterations:3 --join-order-optimizer:exhaustive --warmup \
                    >> ${log_file}

                pkill -f store
                sleep 5
            done
        done
    done
done
