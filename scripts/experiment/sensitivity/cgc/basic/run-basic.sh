#!/bin/bash

# Define the possible values for each configuration
cluster_algorithms=("AGGLOMERATIVE")
cost_models=("OURS" "JACCARD")
layering_orders=("DESCENDING")

# Define target and log directories
target_dir_base="/data/dbpedia/"
log_dir_base="/turbograph-v3/logs"

# Get current date and time for log directory
current_datetime=$(date +"%Y-%m-%d")
log_dir="${log_dir_base}/query/${current_datetime}"
mkdir -p ${log_dir}

# Input parameters
queries_path="/turbograph-v3/queries/basic/"
query_numbers="1-100"

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

# Loop over all combinations of cost models, distributions, and thresholds
for cluster_algo in "${cluster_algorithms[@]}"; do
    for cost_model in "${cost_models[@]}"; do
        for layering_order in "${layering_orders[@]}"; do
            output_file="basic_${cluster_algo}_${cost_model}_${layering_order}.csv"
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
                log_file="${log_dir}/basic_Q${query_num}_${cluster_algo}_${cost_model}_${layering_order}.txt"

                # Run query
                output_str=$(timeout 3600s \
                    /turbograph-v3/build-release/tools/client \
                    --standalone \
                    --slient \
                    --workspace ${target_dir} \
                    --query "${query_str}" \
                    --disable-merge-join \
                    --iterations 3 \
                    --join-order-optimizer exhaustive \
                    --compile-only \
                    --warmup)

                # Output to log file
                echo "$output_str" >> "$log_file"

                # Extract times
                compile_time=$(echo "$output_str" | grep -oP 'Average Compile Time: \K[0-9.]+' | awk '{printf "%.2f", $1}')
                exec_time=$(echo "$output_str" | grep -oP 'Average Query Execution Time: \K[0-9.]+' | awk '{printf "%.2f", $1}')
                end_to_end_time=$(echo "$output_str" | grep -oP 'Average End to End Time: \K[0-9.]+' | awk '{printf "%.2f", $1}')

                echo "Q$query_num, $compile_time, $exec_time, $end_to_end_time" >> "$output_file"

                sleep 1
            done
        done
    done
done
