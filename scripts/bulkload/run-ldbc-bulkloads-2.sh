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
    
    ${BUILD_DIR}/bulkload \
        --log-level trace \
        --standalone \
        --output_dir ${target_dir} \
        --nodes Comment:Message ${data_dir}/dynamic/Comment.csv \
        --nodes Post:Message ${data_dir}/dynamic/Post.csv
done
