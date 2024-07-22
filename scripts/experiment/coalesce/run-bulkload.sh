#!/bin/bash

# Define the possible values for each configuration
num_schemas=("16")

# Define source, target, and log directories
source_dir_base="/source-data/goodbye/coalesce/"
target_dir_base="/data/goodbye/coalesce/"
log_dir_base="/turbograph-v3/logs/coalesce"

# Loop over all combinations of cluster algorithms, cost models, and layering orders
for num_schema in "${num_schemas[@]}"; do
    source_dir="${source_dir_base}/schema-${num_schema}"
    target_dir="${target_dir_base}/schema-${num_schema}"
    
    rm -rf ${target_dir}
    mkdir -p ${target_dir}
    
    /turbograph-v3/build-release/tbgpp-graph-store/store 200GB &
    /turbograph-v3/build-release/tbgpp-graph-store/catalog_test_catalog_server ${target_dir} &
    sleep 5

    /turbograph-v3/build-release/tbgpp-execution-engine/bulkload_using_map \
        --output_dir:${target_dir} \
        --nodes:Package ${source_dir}/Package.csv \
        --jsonl:"--file_path:${source_dir}/Object_zipf_0.json --nodes:Object" \
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
        --relationships_backward:PACKAGE_COMPOSED_OF_OBJECT ${source_dir}/Package_COMPOSED_OF_Object.csv.backward &> /dev/null
    
    /turbograph-v3/build-release/tbgpp-client/TurboGraph-S62 --workspace:${target_dir} --query:flush_file_meta;
    /turbograph-v3/build-release/tbgpp-client/TurboGraph-S62 --workspace:${target_dir} --query:analyze;

    pkill -f store
    pkill -f catalog_test_catalog_server
    sleep 5
done
