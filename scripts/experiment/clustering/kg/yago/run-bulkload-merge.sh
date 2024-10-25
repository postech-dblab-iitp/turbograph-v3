#!/bin/bash

# Define the possible values for each configuration
# cluster_algorithms=("DBSCAN" "AGGLOMERATIVE" "GMM")
# cost_models=("OURS" "OVERLAP" "JACCARD" "WEIGHTEDJACCARD" "COSINE" "DICE")
# layering_orders=("ASCENDING" "DESCENDING" "NO_SORT")
cluster_algorithms=("AGGLOMERATIVE")
cost_models=("OURS")
layering_orders=("DESCENDING")
merge_modes=("IN_STORAGE")

# File path to the configuration header
config_file_path="/turbograph-v3/tbgpp-common/include/common/graph_simdjson_parser.hpp"

# Define source, target, and log directories
source_dir_base="/source-data/yago/"
target_dir_base="/data/yago/"
log_dir_base="/turbograph-v3/logs"

# Function to update the configuration file with new values
update_config_file() {
    local cluster_algo=$1
    local cost_model=$2
    local layering_order=$3
    local merge=$4

    # Use sed to update the configuration file
    sed -i "s/const ClusterAlgorithmType cluster_algo_type = .*/const ClusterAlgorithmType cluster_algo_type = ClusterAlgorithmType::${cluster_algo};/" ${config_file_path}
    sed -i "s/const CostModel cost_model = .*/const CostModel cost_model = CostModel::${cost_model};/" ${config_file_path}
    if [ "${cluster_algo}" == "AGGLOMERATIVE" ]; then
        sed -i "s/const LayeringOrder layering_order = .*/const LayeringOrder layering_order = LayeringOrder::${layering_order};/" ${config_file_path}
    else
        sed -i "s/const LayeringOrder layering_order = .*/const LayeringOrder layering_order = LayeringOrder::DESCENDING;/" ${config_file_path} # Default value
    fi
    sed -i "s/const MergeInAdvance merge_in_advance = .*/const MergeInAdvance merge_in_advance = MergeInAdvance::${merge};/" ${config_file_path}
}

# Get current date and time for log directory
current_datetime=$(date +"%Y-%m-%d")
log_dir="${log_dir_base}/bulkload/${current_datetime}"
mkdir -p ${log_dir}

# Loop over all combinations of cluster algorithms, cost models, and layering orders
for cluster_algo in "${cluster_algorithms[@]}"; do
    for cost_model in "${cost_models[@]}"; do
        for layering_order in "${layering_orders[@]}"; do
            for merge in "${merge_modes[@]}"; do
                update_config_file ${cluster_algo} ${cost_model} ${layering_order} ${merge}
                cd /turbograph-v3/build-release && ninja

                source_dir="${source_dir_base}"
                target_dir="${target_dir_base}/yago_${cluster_algo}_${cost_model}_${layering_order}_${merge}"
                
                rm -rf ${target_dir}
                mkdir -p ${target_dir}
                
                /turbograph-v3/build-release/tbgpp-graph-store/store 365GB &
                /turbograph-v3/build-release/tbgpp-graph-store/catalog_test_catalog_server ${target_dir} &
                sleep 5

                log_file="${log_dir}/yago_${i}_${cluster_algo}_${cost_model}_${layering_order}_${merge}.txt"
                /turbograph-v3/build-release/tbgpp-execution-engine/bulkload_using_map \
                    --output_dir:${target_dir} \
                    --jsonl:"--file_path:${source_dir}/nodes.json --nodes:NODE" \
                    --relationships:http://schema.org/gender ${source_dir}/edges_gender.csv \
                    --relationships_backward:http://schema.org/gender ${source_dir}/edges_gender.csv.backward \
                    --relationships:http://schema.org/manufacturer ${source_dir}/edges_manufacturer.csv \
                    --relationships_backward:http://schema.org/manufacturer ${source_dir}/edges_manufacturer.csv.backward \
                    --relationships:http://www.w3.org/1999/02/22-rdf-syntax-ns#first ${source_dir}/edges_22-rdf-syntax-ns#first.csv \
                    --relationships_backward:http://www.w3.org/1999/02/22-rdf-syntax-ns#first ${source_dir}/edges_22-rdf-syntax-ns#first.csv.backward \
                    --relationships:http://www.w3.org/1999/02/22-rdf-syntax-ns#type ${source_dir}/edges_22-rdf-syntax-ns#type.csv \
                    --relationships_backward:http://www.w3.org/1999/02/22-rdf-syntax-ns#type ${source_dir}/edges_22-rdf-syntax-ns#type.csv.backward \
                    --relationships:http://schema.org/inLanguage ${source_dir}/edges_inLanguage.csv \
                    --relationships_backward:http://schema.org/inLanguage ${source_dir}/edges_inLanguage.csv.backward \
                    --relationships:http://schema.org/location ${source_dir}/edges_location.csv \
                    --relationships_backward:http://schema.org/location ${source_dir}/edges_location.csv.backward \
                    --relationships:http://www.w3.org/2002/07/owl#sameAs ${source_dir}/edges_owl#sameAs.csv \
                    --relationships_backward:http://www.w3.org/2002/07/owl#sameAs ${source_dir}/edges_owl#sameAs.csv.backward \
                    --relationships:http://www.w3.org/1999/02/22-rdf-syntax-ns#rest ${source_dir}/edges_22-rdf-syntax-ns#rest.csv \
                    --relationships_backward:http://www.w3.org/1999/02/22-rdf-syntax-ns#rest ${source_dir}/edges_22-rdf-syntax-ns#rest.csv.backward \
                    --relationships:http://schema.org/locationCreated ${source_dir}/edges_locationCreated.csv \
                    --relationships_backward:http://schema.org/locationCreated ${source_dir}/edges_locationCreated.csv.backward \
                    --relationships:http://schema.org/homeLocation ${source_dir}/edges_homeLocation.csv \
                    --relationships_backward:http://schema.org/homeLocation ${source_dir}/edges_homeLocation.csv.backward \
                    --relationships:http://schema.org/spouse ${source_dir}/edges_spouse.csv \
                    --relationships_backward:http://schema.org/spouse ${source_dir}/edges_spouse.csv.backward \
                    --relationships:http://schema.org/birthPlace ${source_dir}/edges_birthPlace.csv \
                    --relationships_backward:http://schema.org/birthPlace ${source_dir}/edges_birthPlace.csv.backward \
                    --relationships:http://schema.org/material ${source_dir}/edges_material.csv \
                    --relationships_backward:http://schema.org/material ${source_dir}/edges_material.csv.backward \
                    --relationships:http://schema.org/author ${source_dir}/edges_author.csv \
                    --relationships_backward:http://schema.org/author ${source_dir}/edges_author.csv.backward \
                    --relationships:http://schema.org/children ${source_dir}/edges_children.csv \
                    --relationships_backward:http://schema.org/children ${source_dir}/edges_children.csv.backward \
                    --relationships:http://schema.org/about ${source_dir}/edges_about.csv \
                    --relationships_backward:http://schema.org/about ${source_dir}/edges_about.csv.backward \
                    --relationships:http://schema.org/memberOf ${source_dir}/edges_memberOf.csv \
                    --relationships_backward:http://schema.org/memberOf ${source_dir}/edges_memberOf.csv.backward \
                    --relationships:http://schema.org/nationality ${source_dir}/edges_nationality.csv \
                    --relationships_backward:http://schema.org/nationality ${source_dir}/edges_nationality.csv.backward \
                    --relationships:http://yago-knowledge.org/resource/ownedBy ${source_dir}/edges_ownedBy.csv \
                    --relationships_backward:http://yago-knowledge.org/resource/ownedBy ${source_dir}/edges_ownedBy.csv.backward \
                    --relationships:http://schema.org/knowsLanguage ${source_dir}/edges_knowsLanguage.csv \
                    --relationships_backward:http://schema.org/knowsLanguage ${source_dir}/edges_knowsLanguage.csv.backward \
                    --relationships:http://schema.org/worksFor ${source_dir}/edges_worksFor.csv \
                    --relationships_backward:http://schema.org/worksFor ${source_dir}/edges_worksFor.csv.backward \
                    --relationships:http://yago-knowledge.org/resource/replaces ${source_dir}/edges_replaces.csv \
                    --relationships_backward:http://yago-knowledge.org/resource/replaces ${source_dir}/edges_replaces.csv.backward \
                    --relationships:http://schema.org/affiliation ${source_dir}/edges_affiliation.csv \
                    --relationships_backward:http://schema.org/affiliation ${source_dir}/edges_affiliation.csv.backward \
                    --relationships:http://schema.org/alumniOf ${source_dir}/edges_alumniOf.csv \
                    --relationships_backward:http://schema.org/alumniOf ${source_dir}/edges_alumniOf.csv.backward \
                    --relationships:http://schema.org/parentTaxon ${source_dir}/edges_parentTaxon.csv \
                    --relationships_backward:http://schema.org/parentTaxon ${source_dir}/edges_parentTaxon.csv.backward \
                    --relationships:http://yago-knowledge.org/resource/neighbors ${source_dir}/edges_neighbors.csv \
                    --relationships_backward:http://yago-knowledge.org/resource/neighbors ${source_dir}/edges_neighbors.csv.backward \
                    --relationships:http://schema.org/actor ${source_dir}/edges_actor.csv \
                    --relationships_backward:http://schema.org/actor ${source_dir}/edges_actor.csv.backward \
                    --relationships:http://schema.org/director ${source_dir}/edges_director.csv \
                    --relationships_backward:http://schema.org/director ${source_dir}/edges_director.csv.backward \
                    --relationships:http://schema.org/founder ${source_dir}/edges_founder.csv \
                    --relationships_backward:http://schema.org/founder ${source_dir}/edges_founder.csv.backward \
                    --relationships:http://schema.org/musicBy ${source_dir}/edges_musicBy.csv \
                    --relationships_backward:http://schema.org/musicBy ${source_dir}/edges_musicBy.csv.backward \
                    --relationships:http://yago-knowledge.org/resource/beliefSystem ${source_dir}/edges_beliefSystem.csv \
                    --relationships_backward:http://yago-knowledge.org/resource/beliefSystem ${source_dir}/edges_beliefSystem.csv.backward \
                    --relationships:http://yago-knowledge.org/resource/appearsIn ${source_dir}/edges_appearsIn.csv \
                    --relationships_backward:http://yago-knowledge.org/resource/appearsIn ${source_dir}/edges_appearsIn.csv.backward \
                    --relationships:http://yago-knowledge.org/resource/flowsInto ${source_dir}/edges_flowsInto.csv \
                    --relationships_backward:http://yago-knowledge.org/resource/flowsInto ${source_dir}/edges_flowsInto.csv.backward \
                    --relationships:http://schema.org/deathPlace ${source_dir}/edges_deathPlace.csv \
                    --relationships_backward:http://schema.org/deathPlace ${source_dir}/edges_deathPlace.csv.backward \
                    --relationships:http://schema.org/award ${source_dir}/edges_award.csv \
                    --relationships_backward:http://schema.org/award ${source_dir}/edges_award.csv.backward \
                    --relationships:http://www.w3.org/ns/shacl#class ${source_dir}/edges_shacl#class.csv \
                    --relationships_backward:http://www.w3.org/ns/shacl#class ${source_dir}/edges_shacl#class.csv.backward \
                    --relationships:http://www.w3.org/ns/shacl#path ${source_dir}/edges_shacl#path.csv \
                    --relationships_backward:http://www.w3.org/ns/shacl#path ${source_dir}/edges_shacl#path.csv.backward \
                    --relationships:http://yago-knowledge.org/schema#fromProperty ${source_dir}/edges_schema#fromProperty.csv \
                    --relationships_backward:http://yago-knowledge.org/schema#fromProperty ${source_dir}/edges_schema#fromProperty.csv.backward \
                    --relationships:http://www.w3.org/ns/shacl#or ${source_dir}/edges_shacl#or.csv \
                    --relationships_backward:http://www.w3.org/ns/shacl#or ${source_dir}/edges_shacl#or.csv.backward \
                    --relationships:http://www.w3.org/ns/shacl#datatype ${source_dir}/edges_shacl#datatype.csv \
                    --relationships_backward:http://www.w3.org/ns/shacl#datatype ${source_dir}/edges_shacl#datatype.csv.backward \
                    --relationships:http://schema.org/owns ${source_dir}/edges_owns.csv \
                    --relationships_backward:http://schema.org/owns ${source_dir}/edges_owns.csv.backward \
                    --relationships:http://yago-knowledge.org/resource/participant ${source_dir}/edges_participant.csv \
                    --relationships_backward:http://yago-knowledge.org/resource/participant ${source_dir}/edges_participant.csv.backward \
                    --relationships:http://schema.org/contentLocation ${source_dir}/edges_contentLocation.csv \
                    --relationships_backward:http://schema.org/contentLocation ${source_dir}/edges_contentLocation.csv.backward \
                    --relationships:http://yago-knowledge.org/resource/follows ${source_dir}/edges_follows.csv \
                    --relationships_backward:http://yago-knowledge.org/resource/follows ${source_dir}/edges_follows.csv.backward \
                    --relationships:http://www.w3.org/ns/shacl#property ${source_dir}/edges_shacl#property.csv \
                    --relationships_backward:http://www.w3.org/ns/shacl#property ${source_dir}/edges_shacl#property.csv.backward \
                    --relationships:http://yago-knowledge.org/schema#fromClass ${source_dir}/edges_schema#fromClass.csv \
                    --relationships_backward:http://yago-knowledge.org/schema#fromClass ${source_dir}/edges_schema#fromClass.csv.backward \
                    --relationships:http://www.w3.org/2000/01/rdf-schema#subClassOf ${source_dir}/edges_rdf-schema#subClassOf.csv \
                    --relationships_backward:http://www.w3.org/2000/01/rdf-schema#subClassOf ${source_dir}/edges_rdf-schema#subClassOf.csv.backward \
                    --relationships:http://yago-knowledge.org/resource/academicDegree ${source_dir}/edges_academicDegree.csv \
                    --relationships_backward:http://yago-knowledge.org/resource/academicDegree ${source_dir}/edges_academicDegree.csv.backward \
                    --relationships:http://schema.org/superEvent ${source_dir}/edges_superEvent.csv \
                    --relationships_backward:http://schema.org/superEvent ${source_dir}/edges_superEvent.csv.backward \
                    --relationships:http://yago-knowledge.org/resource/playsIn ${source_dir}/edges_playsIn.csv \
                    --relationships_backward:http://yago-knowledge.org/resource/playsIn ${source_dir}/edges_playsIn.csv.backward \
                    --relationships:http://www.w3.org/2002/07/owl#disjointWith ${source_dir}/edges_owl#disjointWith.csv \
                    --relationships_backward:http://www.w3.org/2002/07/owl#disjointWith ${source_dir}/edges_owl#disjointWith.csv.backward \
                    --relationships:http://schema.org/productionCompany ${source_dir}/edges_productionCompany.csv \
                    --relationships_backward:http://schema.org/productionCompany ${source_dir}/edges_productionCompany.csv.backward \
                    --relationships:http://www.w3.org/2000/01/rdf-schema#subPropertyOf ${source_dir}/edges_rdf-schema#subPropertyOf.csv \
                    --relationships_backward:http://www.w3.org/2000/01/rdf-schema#subPropertyOf ${source_dir}/edges_rdf-schema#subPropertyOf.csv.backward \
                    --relationships:http://yago-knowledge.org/resource/candidateIn ${source_dir}/edges_candidateIn.csv \
                    --relationships_backward:http://yago-knowledge.org/resource/candidateIn ${source_dir}/edges_candidateIn.csv.backward \
                    --relationships:http://yago-knowledge.org/resource/conferredBy ${source_dir}/edges_conferredBy.csv \
                    --relationships_backward:http://yago-knowledge.org/resource/conferredBy ${source_dir}/edges_conferredBy.csv.backward \
                    --relationships:http://schema.org/publisher ${source_dir}/edges_publisher.csv \
                    --relationships_backward:http://schema.org/publisher ${source_dir}/edges_publisher.csv.backward \
                    --relationships:http://yago-knowledge.org/resource/capital ${source_dir}/edges_capital.csv \
                    --relationships_backward:http://yago-knowledge.org/resource/capital ${source_dir}/edges_capital.csv.backward \
                    --relationships:http://yago-knowledge.org/resource/notableWork ${source_dir}/edges_notableWork.csv \
                    --relationships_backward:http://yago-knowledge.org/resource/notableWork ${source_dir}/edges_notableWork.csv.backward \
                    --relationships:http://yago-knowledge.org/resource/highestPoint ${source_dir}/edges_highestPoint.csv \
                    --relationships_backward:http://yago-knowledge.org/resource/highestPoint ${source_dir}/edges_highestPoint.csv.backward \
                    --relationships:http://yago-knowledge.org/resource/doctoralAdvisor ${source_dir}/edges_doctoralAdvisor.csv \
                    --relationships_backward:http://yago-knowledge.org/resource/doctoralAdvisor ${source_dir}/edges_doctoralAdvisor.csv.backward \
                    --relationships:http://schema.org/lyricist ${source_dir}/edges_lyricist.csv \
                    --relationships_backward:http://schema.org/lyricist ${source_dir}/edges_lyricist.csv.backward \
                    --relationships:http://schema.org/performer ${source_dir}/edges_performer.csv \
                    --relationships_backward:http://schema.org/performer ${source_dir}/edges_performer.csv.backward \
                    --relationships:http://schema.org/organizer ${source_dir}/edges_organizer.csv \
                    --relationships_backward:http://schema.org/organizer ${source_dir}/edges_organizer.csv.backward \
                    --relationships:http://yago-knowledge.org/resource/influencedBy ${source_dir}/edges_influencedBy.csv \
                    --relationships_backward:http://yago-knowledge.org/resource/influencedBy ${source_dir}/edges_influencedBy.csv.backward \
                    --relationships:http://yago-knowledge.org/resource/leader ${source_dir}/edges_leader.csv \
                    --relationships_backward:http://yago-knowledge.org/resource/leader ${source_dir}/edges_leader.csv.backward \
                    --relationships:http://yago-knowledge.org/resource/terminus ${source_dir}/edges_terminus.csv \
                    --relationships_backward:http://yago-knowledge.org/resource/terminus ${source_dir}/edges_terminus.csv.backward \
                    --relationships:http://yago-knowledge.org/resource/parentBody ${source_dir}/edges_parentBody.csv \
                    --relationships_backward:http://yago-knowledge.org/resource/parentBody ${source_dir}/edges_parentBody.csv.backward \
                    --relationships:http://schema.org/sponsor ${source_dir}/edges_sponsor.csv \
                    --relationships_backward:http://schema.org/sponsor ${source_dir}/edges_sponsor.csv.backward \
                    --relationships:http://schema.org/illustrator ${source_dir}/edges_illustrator.csv \
                    --relationships_backward:http://schema.org/illustrator ${source_dir}/edges_illustrator.csv.backward \
                    --relationships:http://schema.org/editor ${source_dir}/edges_editor.csv \
                    --relationships_backward:http://schema.org/editor ${source_dir}/edges_editor.csv.backward \
                    --relationships:http://yago-knowledge.org/resource/director ${source_dir}/edges_director.csv \
                    --relationships_backward:http://yago-knowledge.org/resource/director ${source_dir}/edges_director.csv.backward \
                    --relationships:http://yago-knowledge.org/resource/studentOf ${source_dir}/edges_studentOf.csv \
                    --relationships_backward:http://yago-knowledge.org/resource/studentOf ${source_dir}/edges_studentOf.csv.backward \
                    --relationships:http://schema.org/recordLabel ${source_dir}/edges_recordLabel.csv \
                    --relationships_backward:http://schema.org/recordLabel ${source_dir}/edges_recordLabel.csv.backward \
                    --relationships:http://yago-knowledge.org/resource/consumes ${source_dir}/edges_consumes.csv \
                    --relationships_backward:http://yago-knowledge.org/resource/consumes ${source_dir}/edges_consumes.csv.backward \
                    --relationships:http://yago-knowledge.org/resource/lowestPoint ${source_dir}/edges_lowestPoint.csv \
                    --relationships_backward:http://yago-knowledge.org/resource/lowestPoint ${source_dir}/edges_lowestPoint.csv.backward \
                    --relationships:http://yago-knowledge.org/resource/officialLanguage ${source_dir}/edges_officialLanguage.csv \
                    --relationships_backward:http://yago-knowledge.org/resource/officialLanguage ${source_dir}/edges_officialLanguage.csv.backward \
                    --relationships:http://www.w3.org/2000/01/rdf-schema#domain ${source_dir}/edges_rdf-schema#domain.csv \
                    --relationships_backward:http://www.w3.org/2000/01/rdf-schema#domain ${source_dir}/edges_rdf-schema#domain.csv.backward \
                    --relationships:http://www.w3.org/2000/01/rdf-schema#range ${source_dir}/edges_rdf-schema#range.csv \
                    --relationships_backward:http://www.w3.org/2000/01/rdf-schema#range ${source_dir}/edges_rdf-schema#range.csv.backward   &> ${log_file}
                            
                pkill -f store
                pkill -f catalog_test_catalog_server
                sleep 5
            done 
        done
    done
done
