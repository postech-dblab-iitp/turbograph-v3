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
                --relationships_backward http://dbpedia.org/ontology/wikiPageRedirects ${source_dir}/edges_wikiPageRedirects_3801.csv.backward \
                --relationships http://dbpedia.org/property/homepage ${source_dir}/edges_homepage_589.csv \
                --relationships_backward http://dbpedia.org/property/homepage ${source_dir}/edges_homepage_589.csv.backward \
                --relationships http://xmlns.com/foaf/0.1/homepage ${source_dir}/edges_homepage_7372.csv \
                --relationships_backward http://xmlns.com/foaf/0.1/homepage ${source_dir}/edges_homepage_7372.csv.backward \
                --relationships http://www.w3.org/1999/02/22-rdf-syntax-ns#type ${source_dir}/edges_22-rdf-syntax-ns#type_6803.csv \
                --relationships_backward http://www.w3.org/1999/02/22-rdf-syntax-ns#type ${source_dir}/edges_22-rdf-syntax-ns#type_6803.csv.backward \
                --relationships http://dbpedia.org/property/website ${source_dir}/edges_website_315.csv \
                --relationships_backward http://dbpedia.org/property/website ${source_dir}/edges_website_315.csv.backward \
                --relationships http://dbpedia.org/ontology/foundationPlace ${source_dir}/edges_foundationPlace_3010.csv \
                --relationships_backward http://dbpedia.org/ontology/foundationPlace ${source_dir}/edges_foundationPlace_3010.csv.backward \
                --relationships http://dbpedia.org/ontology/developer ${source_dir}/edges_developer_3910.csv \
                --relationships_backward http://dbpedia.org/ontology/developer ${source_dir}/edges_developer_3910.csv.backward \
                --relationships http://dbpedia.org/ontology/nationality ${source_dir}/edges_nationality_797.csv \
                --relationships_backward http://dbpedia.org/ontology/nationality ${source_dir}/edges_nationality_797.csv.backward \
                --relationships http://dbpedia.org/property/clubs ${source_dir}/edges_clubs_7515.csv \
                --relationships_backward http://dbpedia.org/property/clubs ${source_dir}/edges_clubs_7515.csv.backward \
                --relationships http://dbpedia.org/ontology/birthPlace ${source_dir}/edges_birthPlace_2950.csv \
                --relationships_backward http://dbpedia.org/ontology/birthPlace ${source_dir}/edges_birthPlace_2950.csv.backward \
                --relationships http://purl.org/dc/terms/subject ${source_dir}/edges_subject_5592.csv \
                --relationships_backward http://purl.org/dc/terms/subject ${source_dir}/edges_subject_5592.csv.backward \
                --relationships http://dbpedia.org/ontology/thumbnail ${source_dir}/edges_thumbnail_4765.csv \
                --relationships_backward http://dbpedia.org/ontology/thumbnail ${source_dir}/edges_thumbnail_4765.csv.backward \
                --relationships http://dbpedia.org/ontology/city ${source_dir}/edges_city_55.csv \
                --relationships_backward http://dbpedia.org/ontology/city ${source_dir}/edges_city_55.csv.backward \
                --relationships http://www.w3.org/2002/07/owl#sameAs ${source_dir}/edges_owl#sameAs_9037.csv \
                --relationships_backward http://www.w3.org/2002/07/owl#sameAs ${source_dir}/edges_owl#sameAs_9037.csv.backward \
                --relationships http://xmlns.com/foaf/0.1/depiction ${source_dir}/edges_depiction_6991.csv \
                --relationships_backward http://xmlns.com/foaf/0.1/depiction ${source_dir}/edges_depiction_6991.csv.backward \
                --relationships http://dbpedia.org/ontology/country ${source_dir}/edges_country_8183.csv \
                --relationships_backward http://dbpedia.org/ontology/country ${source_dir}/edges_country_8183.csv.backward \
                --relationships http://dbpedia.org/ontology/countryOrigin ${source_dir}/edges_countryOrigin_7466.csv \
                --relationships_backward http://dbpedia.org/ontology/countryOrigin ${source_dir}/edges_countryOrigin_7466.csv.backward \
                --relationships http://dbpedia.org/ontology/birthPlace ${source_dir}/edges_birthPlace_2950.csv \
                --relationships_backward http://dbpedia.org/ontology/birthPlace ${source_dir}/edges_birthPlace_2950.csv.backward \
                --relationships http://dbpedia.org/ontology/keyPerson ${source_dir}/edges_keyPerson_285.csv \
                --relationships_backward http://dbpedia.org/ontology/keyPerson ${source_dir}/edges_keyPerson_285.csv.backward \
                --relationships http://dbpedia.org/ontology/location ${source_dir}/edges_location_4045.csv \
                --relationships_backward http://dbpedia.org/ontology/location ${source_dir}/edges_location_4045.csv.backward \
                --relationships http://dbpedia.org/ontology/locationCity ${source_dir}/edges_locationCity_1514.csv \
                --relationships_backward http://dbpedia.org/ontology/locationCity ${source_dir}/edges_locationCity_1514.csv.backward \
                --relationships http://dbpedia.org/ontology/locationCountry ${source_dir}/edges_locationCountry_1719.csv \
                --relationships_backward http://dbpedia.org/ontology/locationCountry ${source_dir}/edges_locationCountry_1719.csv.backward \
                --relationships http://dbpedia.org/ontology/industry ${source_dir}/edges_industry_6326.csv \
                --relationships_backward http://dbpedia.org/ontology/industry ${source_dir}/edges_industry_6326.csv.backward \
                --relationships http://dbpedia.org/ontology/genre ${source_dir}/edges_genre_3794.csv \
                --relationships_backward http://dbpedia.org/ontology/genre ${source_dir}/edges_genre_3794.csv.backward \
                --relationships http://dbpedia.org/ontology/musicBy ${source_dir}/edges_musicBy_6854.csv \
                --relationships_backward http://dbpedia.org/ontology/musicBy ${source_dir}/edges_musicBy_6854.csv.backward \
                --relationships http://dbpedia.org/ontology/musicComposer ${source_dir}/edges_musicComposer_4171.csv \
                --relationships_backward http://dbpedia.org/ontology/musicComposer ${source_dir}/edges_musicComposer_4171.csv.backward \
                --relationships http://dbpedia.org/ontology/musicFusionGenre ${source_dir}/edges_musicFusionGenre_354.csv \
                --relationships_backward http://dbpedia.org/ontology/musicFusionGenre ${source_dir}/edges_musicFusionGenre_354.csv.backward \
                --relationships http://dbpedia.org/ontology/musicSubgenre ${source_dir}/edges_musicSubgenre_7829.csv \
                --relationships_backward http://dbpedia.org/ontology/musicSubgenre ${source_dir}/edges_musicSubgenre_7829.csv.backward \
                --relationships http://dbpedia.org/ontology/musicalArtist ${source_dir}/edges_musicalArtist_8387.csv \
                --relationships_backward http://dbpedia.org/ontology/musicalArtist ${source_dir}/edges_musicalArtist_8387.csv.backward \
                --relationships http://dbpedia.org/ontology/musicalBand ${source_dir}/edges_musicalBand_8808.csv \
                --relationships_backward http://dbpedia.org/ontology/musicalBand ${source_dir}/edges_musicalBand_8808.csv.backward \
                --relationships http://dbpedia.org/ontology/stylisticOrigin ${source_dir}/edges_stylisticOrigin_3904.csv \
                --relationships_backward http://dbpedia.org/ontology/stylisticOrigin ${source_dir}/edges_stylisticOrigin_3904.csv.backward \
                --relationships http://dbpedia.org/ontology/subregion ${source_dir}/edges_subregion_6627.csv \
                --relationships_backward http://dbpedia.org/ontology/subregion ${source_dir}/edges_subregion_6627.csv.backward \
                --relationships http://dbpedia.org/property/album ${source_dir}/edges_album_5285.csv \
                --relationships_backward http://dbpedia.org/property/album ${source_dir}/edges_album_5285.csv.backward&> ${log_file}

            pkill -f store
            sleep 5
        done
    done
done
