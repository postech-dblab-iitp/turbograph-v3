#!/bin/bash

# Define the possible values for each configuration
# cluster_algorithms=("DBSCAN" "AGGLOMERATIVE" "GMM")
# cost_models=("OURS" "OVERLAP" "JACCARD" "WEIGHTEDJACCARD" "COSINE" "DICE")
# layering_orders=("ASCENDING" "DESCENDING" "NO_SORT")
cluster_algorithms=("AGGLOMERATIVE")
cost_models=("JACCARD")
layering_orders=("DESCENDING")

# File path to the configuration header
config_file_path="/turbograph-v3/src/include/common/graph_simdjson_parser.hpp"

# Define source, target, and log directories
source_dir_base="/source-data/dbpedia/"
target_dir_base="/data/dbpedia/"
log_dir_base="/turbograph-v3/logs"

# Function to update the configuration file with new values
update_config_file() {
    local cluster_algo=$1
    local cost_model=$2
    local layering_order=$3

    # Use sed to update the configuration file
    sed -i "s/const ClusterAlgorithmType cluster_algo_type = .*/const ClusterAlgorithmType cluster_algo_type = ClusterAlgorithmType::${cluster_algo};/" ${config_file_path}
    sed -i "s/const CostModel cost_model = .*/const CostModel cost_model = CostModel::${cost_model};/" ${config_file_path}
    if [ "${cluster_algo}" == "AGGLOMERATIVE" ]; then
        sed -i "s/const LayeringOrder layering_order = .*/const LayeringOrder layering_order = LayeringOrder::${layering_order};/" ${config_file_path}
    else
        sed -i "s/const LayeringOrder layering_order = .*/const LayeringOrder layering_order = LayeringOrder::DESCENDING;/" ${config_file_path} # Default value
    fi
}

# Get current date and time for log directory
current_datetime=$(date +"%Y-%m-%d")
log_dir="${log_dir_base}/bulkload/${current_datetime}"
mkdir -p ${log_dir}

# Loop over all combinations of cluster algorithms, cost models, and layering orders
for cluster_algo in "${cluster_algorithms[@]}"; do
    for cost_model in "${cost_models[@]}"; do
        for layering_order in "${layering_orders[@]}"; do
            update_config_file ${cluster_algo} ${cost_model} ${layering_order}
            cd /turbograph-v3/build-release && ninja

            source_dir="${source_dir_base}"
            target_dir="${target_dir_base}/dbpedia_${cluster_algo}_${cost_model}_${layering_order}"

            /turbograph-v3/build-release/tools/store 500GB &
            sleep 15

            log_file="${log_dir}/dbpedia_${cluster_algo}_${cost_model}_${layering_order}.txt"
            /turbograph-v3/build-release/tools/bulkload \
                --log-level info \
                --skip-histogram \
                --output_dir ${target_dir} \
                --nodes NODE ${source_dir}/nodes.json \
                --relationships http://dbpedia.org/ontology/wikiPageRedirects ${source_dir}/edges_wikiPageRedirects_3801.csv \
                --relationships_backward http://dbpedia.org/ontology/wikiPageRedirects ${source_dir}/edges_wikiPageRedirects_3801.csv.backward &> ${log_file}

            pkill -f store
            sleep 5
        done
    done
done
