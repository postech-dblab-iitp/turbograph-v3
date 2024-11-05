#!/bin/bash

# Define the possible values for each configuration
cluster_algorithms=("AGGLOMERATIVE" "SINGLECLUSTER")
cost_models=("OURS")
layering_orders=("DESCENDING")
merge_modes=("" "IN_STORAGE")

# Define target and log directories
scale_factor=10
target_dir_base="/data/yago/"
log_dir_base="/turbograph-v3/logs"

# Get current date and time for log directory
current_datetime=$(date +"%Y-%m-%d")
log_dir="${log_dir_base}/query/${current_datetime}"
mkdir -p ${log_dir}

# Input parameters
queries_path="/turbograph-v3/queries/kg/yago-incremental/"
query_list=$1  # The list of queries, e.g., "Q_chain_2_1;Q_chain3_1"


# Function to parse query list
parse_query_list() {
    IFS=';' read -ra QUERIES <<< "$1"
    for query in "${QUERIES[@]}"; do
        echo $query
    done
}

# Parse query list
queries=$(parse_query_list "$query_list")

# Loop over all combinations of cost models, distributions, and thresholds
for cluster_algo in "${cluster_algorithms[@]}"; do
    for cost_model in "${cost_models[@]}"; do
        for layering_order in "${layering_orders[@]}"; do
            for merge in "${merge_modes[@]}"; do
                for query_name in $queries; do
                    query_file="${queries_path}/${query_name}.cql"
                    if [ ! -f "$query_file" ]; then
                        echo "Query file $query_file not found!"
                        continue
                    fi
                    query_str=$(cat "$query_file")

                    # Setup
                    if [ -z "$merge" ]; then
                        target_dir="${target_dir_base}/yago_${cluster_algo}_${cost_model}_${layering_order}"
                        log_file="${log_dir}/yago_Q_${query_name}_${cluster_algo}_${cost_model}_${layering_order}.txt"
                    else
                        target_dir="${target_dir_base}/yago_${cluster_algo}_${cost_model}_${layering_order}_${merge}"
                        log_file="${log_dir}/yago_Q_${query_name}_${cluster_algo}_${cost_model}_${layering_order}_${merge}.txt"
                    fi

                    if [ ! -d "$target_dir" ]; then
                        echo "Target directory $target_dir not found!"
                        continue
                    fi

                    # Run store
                    /turbograph-v3/build-release/tbgpp-graph-store/store 365GB&
                    sleep 5

                    # Run query
                    timeout 3600s \
                        /turbograph-v3/build-release/tbgpp-client/TurboGraph-S62 --workspace:${target_dir} --query:"$query_str" --disable-merge-join --num-iterations:4 --join-order-optimizer:exhaustive --explain --profile --warmup \
                        > ${log_file} 2>&1

                    pkill -f store
                    sleep 5
                done
            done
        done
    done
done
