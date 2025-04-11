#!/bin/bash

# Define the possible values for each configuration
# cluster_algorithms=("DBSCAN" "AGGLOMERATIVE" "GMM")
# cost_models=("OURS" "OVERLAP" "JACCARD" "WEIGHTEDJACCARD" "COSINE" "DICE")
# layering_orders=("ASCENDING" "DESCENDING" "NO_SORT")
cluster_algorithms=("AGGLOMERATIVE")
cost_models=("OURS")
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
            target_dir="${target_dir_base}/hops_${cluster_algo}_${cost_model}_${layering_order}"

            /turbograph-v3/build-release/tools/store 500GB &
            sleep 15

            log_file="${log_dir}/hops_${cluster_algo}_${cost_model}_${layering_order}"

            ## POINT files are single direction
            ## Other files are bidirectional
            ## POINT files for shirnking
            ## Others for expansion

            /turbograph-v3/build-release/tools/bulkload \
                --log-level info \
                --skip-histogram \
                --output_dir ${target_dir} \
                --nodes NODE ${source_dir}/nodes.json \
                --relationships DI_POINT_ZERO_ONE_PRCNT ${source_dir}/edges_owl#sameAs_9037_sampled_001_prcnt_di.csv \
                --relationships BI_POINT_ZERO_ONE_PRCNT ${source_dir}/edges_owl#sameAs_9037_sampled_001_prcnt_bi.csv \
                --relationships DI_POINT_ONE_PRCNT ${source_dir}/edges_owl#sameAs_9037_sampled_01_prcnt_di.csv \
                --relationships BI_POINT_ONE_PRCNT ${source_dir}/edges_owl#sameAs_9037_sampled_01_prcnt_bi.csv \
                --relationships DI_POINT_FIVE_PRCNT ${source_dir}/edges_owl#sameAs_9037_sampled_05_prcnt_di.csv \
                --relationships BI_POINT_FIVE_PRCNT ${source_dir}/edges_owl#sameAs_9037_sampled_05_prcnt_bi.csv \
                --relationships BI_ONE_PRCNT ${source_dir}/edges_owl#sameAs_9037_sampled_1_prcnt_bi.csv \
                --relationships DI_ONE_PRCNT ${source_dir}/edges_owl#sameAs_9037_sampled_1_prcnt_di.csv \
                --relationships BI_THREE_PRCNT ${source_dir}/edges_owl#sameAs_9037_sampled_1_prcnt_bi.csv \
                --relationships DI_THREE_PRCNT ${source_dir}/edges_owl#sameAs_9037_sampled_3_prcnt_di.csv \
                --relationships BI_THREE_PRCNT ${source_dir}/edges_owl#sameAs_9037_sampled_3_prcnt_bi.csv \
                --relationships DI_FIVE_PRCNT ${source_dir}/edges_owl#sameAs_9037_sampled_5_prcnt_di.csv \
                --relationships BI_FIVE_PRCNT ${source_dir}/edges_owl#sameAs_9037_sampled_5_prcnt_bi.csv \
                --relationships DI_TEN_PRCNT ${source_dir}/edges_owl#sameAs_9037_sampled_10_prcnt_di.csv \
                --relationships BI_TEN_PRCNT ${source_dir}/edges_owl#sameAs_9037_sampled_10_prcnt_bi.csv \
                --relationships DI_THREE_TEN_PRCNT ${source_dir}/edges_owl#sameAs_9037_sampled_30_prcnt_di.csv \
                --relationships BI_THREE_TEN_PRCNT ${source_dir}/edges_owl#sameAs_9037_sampled_30_prcnt_bi.csv \
                --relationships DI_FIVE_TEN_PRCNT ${source_dir}/edges_owl#sameAs_9037_sampled_50_prcnt_di.csv \
                --relationships BI_FIVE_TEN_PRCNT ${source_dir}/edges_owl#sameAs_9037_sampled_50_prcnt_bi.csv &> ${log_file}

            pkill -f store
            sleep 5
        done
    done
done
