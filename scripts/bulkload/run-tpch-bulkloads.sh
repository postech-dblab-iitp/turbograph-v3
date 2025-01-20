#!/bin/bash

# Define the possible values for each configuration
scale_factors=("1" "10")
source_dir_base="/source-data/tpch/"
target_dir_base="/data/tpch/"
SUFFIX=""

# Loop over all combinations of cluster algorithms, cost models, and layering orders
for scale_factor in "${scale_factors[@]}"; do
    data_dir="${source_dir_base}/s62/sf${scale_factor}"
    target_dir="${target_dir_base}/sf${scale_factor}"
    
    rm -rf ${target_dir}
    mkdir -p ${target_dir}
    
    /s62/build-release/s62-graph-store/store 365GB &
    /s62/build-release/s62-graph-store/catalog_test_catalog_server ${target_dir} &
    sleep 5

    /s62/build-release/s62-execution-engine/bulkload_using_map \
        --output_dir:${target_dir} \
        --nodes:LINEITEM ${data_dir}/lineitem.tbl${SUFFIX} \
        --nodes:ORDERS ${data_dir}/orders.tbl${SUFFIX} \
        --nodes:CUSTOMER ${data_dir}/customer.tbl${SUFFIX} \
        --nodes:NATION ${data_dir}/nation.tbl${SUFFIX} \
        --nodes:REGION ${data_dir}/region.tbl${SUFFIX} \
        --nodes:PART ${data_dir}/part.tbl${SUFFIX} \
        --nodes:SUPPLIER ${data_dir}/supplier.tbl${SUFFIX} \
        --relationships:CUST_BELONG_TO ${data_dir}/customer_belongTo_nation.tbl${SUFFIX} \
        --relationships_backward:CUST_BELONG_TO ${data_dir}/customer_belongTo_nation.tbl${SUFFIX}.backward \
        --relationships:COMPOSED_BY ${data_dir}/lineitem_composedBy_part.tbl${SUFFIX} \
        --relationships_backward:COMPOSED_BY ${data_dir}/lineitem_composedBy_part.tbl${SUFFIX}.backward \
        --relationships:IS_PART_OF ${data_dir}/lineitem_isPartOf_orders.tbl${SUFFIX} \
        --relationships_backward:IS_PART_OF ${data_dir}/lineitem_isPartOf_orders.tbl${SUFFIX}.backward \
        --relationships:SUPPLIED_BY ${data_dir}/lineitem_suppliedBy_supplier.tbl${SUFFIX} \
        --relationships_backward:SUPPLIED_BY ${data_dir}/lineitem_suppliedBy_supplier.tbl${SUFFIX}.backward \
        --relationships:IS_LOCATED_IN ${data_dir}/nation_isLocatedIn_region.tbl${SUFFIX} \
        --relationships_backward:IS_LOCATED_IN ${data_dir}/nation_isLocatedIn_region.tbl${SUFFIX}.backward \
        --relationships:MADE_BY ${data_dir}/orders_madeBy_customer.tbl${SUFFIX} \
        --relationships_backward:MADE_BY ${data_dir}/orders_madeBy_customer.tbl${SUFFIX}.backward \
        --relationships:SUPP_BELONG_TO ${data_dir}/supplier_belongTo_nation.tbl${SUFFIX} \
        --relationships_backward:SUPP_BELONG_TO ${data_dir}/supplier_belongTo_nation.tbl${SUFFIX}.backward \
        --relationships:PARTSUPP ${data_dir}/partsupp.tbl \
        --relationships_backward:PARTSUPP ${data_dir}/partsupp.tbl.backward
        
    /s62/build-release/s62-client/S62-CLI --workspace:${target_dir} --query:flush_file_meta;
    /s62/build-release/s62-client/S62-CLI --workspace:${target_dir} --query:analyze;

    pkill -f store
    pkill -f catalog_test_catalog_server
    sleep 5
done
