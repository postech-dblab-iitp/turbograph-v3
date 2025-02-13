#!/bin/bash

# Define the possible values for each configuration
BUILD_DIR="/turbograph-v3/build-release/tools/"
scale_factors=("1")
source_dir_base="/source-data/ldbc/"
target_dir_base="/data/ldbc/"

# Loop over all combinations of cluster algorithms, cost models, and layering orders
for scale_factor in "${scale_factors[@]}"; do
    data_dir="${source_dir_base}/sf${scale_factor}"
    target_dir="${target_dir_base}/sf${scale_factor}"
    
    rm -rf ${target_dir}
    mkdir -p ${target_dir}
    
    ${BUILD_DIR}/store 365GB &
    ${BUILD_DIR}/catalog_server ${target_dir} &
    sleep 10

    /turbograph-v3/build/tools//bulkload \
        --log-level trace \
        --output_dir ${target_dir} \
        --nodes Person ${data_dir}/dynamic/Person.csv \
        --relationships KNOWS ${data_dir}/dynamic/Person_knows_Person.csv \
        --relationships_backward KNOWS ${data_dir}/dynamic/Person_knows_Person.csv.backward 

    pkill -f store
    pkill -f catalog_server
    sleep 5
done
