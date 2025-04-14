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
queries_path="/turbograph-v3/queries/kg/dbpedia/"
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
            output_file="dbpedia_${cluster_algo}_${cost_model}_${layering_order}.csv"
            echo "QueryNumber,CompileTime,QueryExecutionTime,EndtoEndTime" > $output_file
            for query_num in $queries; do
                query_file="${queries_path}/q${query_num}.cql"
                if [ ! -f "$query_file" ]; then
                    echo "Query file $query_file not found!"
                    continue
                fi
                query_str=$(cat "$query_file")

                # Setup
                target_dir="${target_dir_base}/dbpedia_${cluster_algo}_${cost_model}_${layering_order}"
                log_file="${log_dir}/dbpedia_Q${query_num}_${cluster_algo}_${cost_model}_${layering_order}.txt"

                # Run query
                output_str=$(timeout 3600s \
                    /turbograph-v3/build-release/tools/client \
                    --standalone \
                    --slient \
                    --workspace ${target_dir} \
                    --query "${query_str}" \
                    --disable-merge-join \
                    --iterations 3 \
                    --join-order-optimizer gem \
                    --warmup)

                # Output to log file
                echo "$output_str" >> "$log_file"

                # Extract times
                compile_time=$(echo "$output_str" | grep -oP 'Average Compile Time: \K[0-9.]+' | awk '{printf "%.2f", $1}')
                exec_time=$(echo "$output_str" | grep -oP 'Average Query Execution Time: \K[0-9.]+' | awk '{printf "%.2f", $1}')
                end_to_end_time=$(echo "$output_str" | grep -oP 'Average End to End Time: \K[0-9.]+' | awk '{printf "%.2f", $1}')

                echo "Q$query_num, $compile_time, $exec_time, $end_to_end_time" >> "$output_file"
            done
        done
    done
done
