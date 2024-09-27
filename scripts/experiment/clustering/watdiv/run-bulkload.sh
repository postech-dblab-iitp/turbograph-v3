#!/bin/bash

# Define the possible values for each configuration
# cluster_algorithms=(""DBSCAN" "OPTICS" "AGGLOMERATIVE" "GMM" "PGSE")
# cost_models=("OURS" "OVERLAP" "JACCARD" "WEIGHTEDJACCARD" "COSINE" "DICE" "SETEDIT")
# layering_orders=("ASCENDING" "DESCENDING" "NO_SORT")
cluster_algorithms=("AGGLOMERATIVE")
cost_models=("OURS")
layering_orders=("DESCENDING")

# File path to the configuration header
config_file_path="/turbograph-v3/tbgpp-common/include/common/graph_simdjson_parser.hpp"

# Define source, target, and log directories
scale_factor=1
source_dir_base="/source-data/watdiv/sf${scale_factor}/"
target_dir_base="/data/watdiv/sf${scale_factor}/"
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
            target_dir="${target_dir_base}/watdiv_${cluster_algo}_${cost_model}_${layering_order}"
            
            rm -rf ${target_dir}
            mkdir -p ${target_dir}
            
            /turbograph-v3/build-release/tbgpp-graph-store/store 365GB &
            /turbograph-v3/build-release/tbgpp-graph-store/catalog_test_catalog_server ${target_dir} &
            sleep 5

            log_file="${log_dir}/watdiv_${i}_${cluster_algo}_${cost_model}_${layering_order}.txt"
            /turbograph-v3/build-release/tbgpp-execution-engine/bulkload_using_map \
                --output_dir:${target_dir} \
                --jsonl:"--file_path:${source_dir}/nodes.json --nodes:NODE" \
                --relationships:http://db.uwaterloo.ca/~galuc/wsdbm/friendOf ${source_dir}/edges_friendOf.csv \
                --relationships_backward:http://db.uwaterloo.ca/~galuc/wsdbm/friendOf ${source_dir}/edges_friendOf.csv.backward \
                --relationships:http://schema.org/eligibleRegion ${source_dir}/edges_eligibleRegion.csv \
                --relationships_backward:http://schema.org/eligibleRegion ${source_dir}/edges_eligibleRegion.csv.backward \
                --relationships:http://db.uwaterloo.ca/~galuc/wsdbm/follows ${source_dir}/edges_follows.csv \
                --relationships_backward:http://db.uwaterloo.ca/~galuc/wsdbm/follows ${source_dir}/edges_follows.csv.backward \
                --relationships:http://purl.org/stuff/rev#reviewer ${source_dir}/edges_rev.csv \
                --relationships_backward:http://purl.org/stuff/rev#reviewer ${source_dir}/edges_rev.csv.backward \
                --relationships:http://ogp.me/ns#tag ${source_dir}/edges_ns.csv \
                --relationships_backward:http://ogp.me/ns#tag ${source_dir}/edges_ns.csv.backward \
                --relationships:http://db.uwaterloo.ca/~galuc/wsdbm/subscribes ${source_dir}/edges_subscribes.csv \
                --relationships_backward:http://db.uwaterloo.ca/~galuc/wsdbm/subscribes ${source_dir}/edges_subscribes.csv.backward \
                --relationships:http://db.uwaterloo.ca/~galuc/wsdbm/gender ${source_dir}/edges_gender.csv \
                --relationships_backward:http://db.uwaterloo.ca/~galuc/wsdbm/gender ${source_dir}/edges_gender.csv.backward \
                --relationships:http://purl.org/stuff/rev#hasReview ${source_dir}/edges_rev.csv \
                --relationships_backward:http://purl.org/stuff/rev#hasReview ${source_dir}/edges_rev.csv.backward \
                --relationships:http://db.uwaterloo.ca/~galuc/wsdbm/hasGenre ${source_dir}/edges_hasGenre.csv \
                --relationships_backward:http://db.uwaterloo.ca/~galuc/wsdbm/hasGenre ${source_dir}/edges_hasGenre.csv.backward \
                --relationships:http://db.uwaterloo.ca/~galuc/wsdbm/makesPurchase ${source_dir}/edges_makesPurchase.csv \
                --relationships_backward:http://db.uwaterloo.ca/~galuc/wsdbm/makesPurchase ${source_dir}/edges_makesPurchase.csv.backward \
                --relationships:http://schema.org/actor ${source_dir}/edges_actor.csv \
                --relationships_backward:http://schema.org/actor ${source_dir}/edges_actor.csv.backward \
                --relationships:http://purl.org/goodrelations/includes ${source_dir}/edges_includes.csv \
                --relationships_backward:http://purl.org/goodrelations/includes ${source_dir}/edges_includes.csv.backward \
                --relationships:http://purl.org/dc/terms/Location ${source_dir}/edges_Location.csv \
                --relationships_backward:http://purl.org/dc/terms/Location ${source_dir}/edges_Location.csv.backward \
                --relationships:http://purl.org/goodrelations/offers ${source_dir}/edges_offers.csv \
                --relationships_backward:http://purl.org/goodrelations/offers ${source_dir}/edges_offers.csv.backward \
                --relationships:http://db.uwaterloo.ca/~galuc/wsdbm/purchaseFor ${source_dir}/edges_purchaseFor.csv \
                --relationships_backward:http://db.uwaterloo.ca/~galuc/wsdbm/purchaseFor ${source_dir}/edges_purchaseFor.csv.backward \
                --relationships:http://db.uwaterloo.ca/~galuc/wsdbm/likes ${source_dir}/edges_likes.csv \
                --relationships_backward:http://db.uwaterloo.ca/~galuc/wsdbm/likes ${source_dir}/edges_likes.csv.backward \
                --relationships:http://www.w3.org/1999/02/22-rdf-syntax-ns#type ${source_dir}/edges_22-rdf-syntax-ns.csv \
                --relationships_backward:http://www.w3.org/1999/02/22-rdf-syntax-ns#type ${source_dir}/edges_22-rdf-syntax-ns.csv.backward \
                --relationships:http://xmlns.com/foaf/age ${source_dir}/edges_age.csv \
                --relationships_backward:http://xmlns.com/foaf/age ${source_dir}/edges_age.csv.backward \
                --relationships:http://www.geonames.org/ontology#parentCountry ${source_dir}/edges_ontology.csv \
                --relationships_backward:http://www.geonames.org/ontology#parentCountry ${source_dir}/edges_ontology.csv.backward \
                --relationships:http://schema.org/nationality ${source_dir}/edges_nationality.csv \
                --relationships_backward:http://schema.org/nationality ${source_dir}/edges_nationality.csv.backward \
                --relationships:http://schema.org/language ${source_dir}/edges_language.csv \
                --relationships_backward:http://schema.org/language ${source_dir}/edges_language.csv.backward \
                --relationships:http://schema.org/director ${source_dir}/edges_director.csv \
                --relationships_backward:http://schema.org/director ${source_dir}/edges_director.csv.backward \
                --relationships:http://xmlns.com/foaf/homepage ${source_dir}/edges_homepage.csv \
                --relationships_backward:http://xmlns.com/foaf/homepage ${source_dir}/edges_homepage.csv.backward \
                --relationships:http://schema.org/employee ${source_dir}/edges_employee.csv \
                --relationships_backward:http://schema.org/employee ${source_dir}/edges_employee.csv.backward \
                --relationships:http://purl.org/ontology/mo/conductor ${source_dir}/edges_conductor.csv \
                --relationships_backward:http://purl.org/ontology/mo/conductor ${source_dir}/edges_conductor.csv.backward \
                --relationships:http://schema.org/author ${source_dir}/edges_author.csv \
                --relationships_backward:http://schema.org/author ${source_dir}/edges_author.csv.backward \
                --relationships:http://purl.org/ontology/mo/performed_in ${source_dir}/edges_performed_in.csv \
                --relationships_backward:http://purl.org/ontology/mo/performed_in ${source_dir}/edges_performed_in.csv.backward \
                --relationships:http://schema.org/contactPoint ${source_dir}/edges_contactPoint.csv \
                --relationships_backward:http://schema.org/contactPoint ${source_dir}/edges_contactPoint.csv.backward \
                --relationships:http://schema.org/editor ${source_dir}/edges_editor.csv \
                --relationships_backward:http://schema.org/editor ${source_dir}/edges_editor.csv.backward \
                --relationships:http://purl.org/ontology/mo/artist ${source_dir}/edges_artist.csv \
                --relationships_backward:http://purl.org/ontology/mo/artist ${source_dir}/edges_artist.csv.backward \
                --relationships:http://schema.org/trailer ${source_dir}/edges_trailer.csv \
                --relationships_backward:http://schema.org/trailer ${source_dir}/edges_trailer.csv.backward &> ${log_file}
            
            /turbograph-v3/build-release/tbgpp-client/TurboGraph-S62 --workspace:${target_dir} --query:flush_file_meta;
            /turbograph-v3/build-release/tbgpp-client/TurboGraph-S62 --workspace:${target_dir} --query:analyze;

            pkill -f store
            pkill -f catalog_test_catalog_server
            sleep 5
        done
    done
done
