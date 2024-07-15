#!/bin/bash

# Define the possible values for each configuration
# cluster_algorithms=(""DBSCAN" "OPTICS" "AGGLOMERATIVE" "GMM" "PGSE")
# cost_models=("OURS" "OVERLAP" "JACCARD" "WEIGHTEDJACCARD" "COSINE" "DICE" "SETEDIT")
# layering_orders=("ASCENDING" "DESCENDING" "NO_SORT")
# distributions=("beta" "exponential" "lognormal" "pareto" "weibull" "zipf")
cluster_algorithms=("DBSCAN" "GMM")
cost_models=("OURS")
layering_orders=("DESCENDING")
distributions=("0" "1" "2")

# File path to the configuration header
config_file_path="/turbograph-v3/tbgpp-common/include/common/graph_simdjson_parser.hpp"

# Define source, target, and log directories
scale_factor=1
source_dir_base="/source-data/goodbye/sf${scale_factor}/"
target_dir_base="/data/goodbye/sf${scale_factor}/"
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

            for i in "${distributions[@]}"; do
                source_dir="${source_dir_base}"
                target_dir="${target_dir_base}/goodbye_zipf_${i}_${cluster_algo}_${cost_model}_${layering_order}"
                
                rm -rf ${target_dir}
                mkdir -p ${target_dir}
                
                /turbograph-v3/build-release/tbgpp-graph-store/store 200GB &
                /turbograph-v3/build-release/tbgpp-graph-store/catalog_test_catalog_server ${target_dir} &
                sleep 5

                log_file="${log_dir}/goodbye_zipf_${i}_${cluster_algo}_${cost_model}_${layering_order}.txt"
                /turbograph-v3/build-release/tbgpp-execution-engine/bulkload_using_map \
                    --output_dir:${target_dir} \
                    --nodes:Package ${source_dir}/Package.csv \
                    --jsonl:"--file_path:${source_dir}/Object_zipf_${i}.json --nodes:Object" \
                    --nodes:Field ${source_dir}/Field.csv \
                    --relationships:OBJECT_CAN_TRANSFORM_INTO_OBJECT ${source_dir}/Object_CAN_TRANSFORM_INTO_Object.csv \
                    --relationships_backward:OBJECT_CAN_TRANSFORM_INTO_OBJECT ${source_dir}/Object_CAN_TRANSFORM_INTO_Object.csv.backward \
                    --relationships:PACKAGE_CAN_TRANSFORM_INTO_PACKAGE ${source_dir}/Package_CAN_TRANSFORM_INTO_Package.csv \
                    --relationships_backward:PACKAGE_CAN_TRANSFORM_INTO_PACKAGE ${source_dir}/Package_CAN_TRANSFORM_INTO_Package.csv.backward \
                    --relationships:FIELD_CAN_TRANSFORM_INTO_FIELD ${source_dir}/Field_CAN_TRANSFORM_INTO_Field.csv \
                    --relationships_backward:FIELD_CAN_TRANSFORM_INTO_FIELD ${source_dir}/Field_CAN_TRANSFORM_INTO_Field.csv.backward \
                    --relationships:OBJECT_COMPOSED_OF_FIELD ${source_dir}/Object_COMPOSED_OF_Field.csv \
                    --relationships_backward:OBJECT_COMPOSED_OF_FIELD ${source_dir}/Object_COMPOSED_OF_Field.csv.backward \
                    --relationships:PACKAGE_COMPOSED_OF_OBJECT ${source_dir}/Package_COMPOSED_OF_Object.csv \
                    --relationships_backward:PACKAGE_COMPOSED_OF_OBJECT ${source_dir}/Package_COMPOSED_OF_Object.csv.backward &> ${log_file}
                
                /turbograph-v3/build-release/tbgpp-client/TurboGraph-S62 --workspace:${target_dir} --query:flush_file_meta;
                /turbograph-v3/build-release/tbgpp-client/TurboGraph-S62 --workspace:${target_dir} --query:analyze;

                pkill -f store
                pkill -f catalog_test_catalog_server
                sleep 5
            done
        done
    done
done
