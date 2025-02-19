#!/bin/bash

# Define the possible values for each configuration
BUILD_DIR="/turbograph-v3/build-release/tools/"
scale_factors=("1")
source_dir_base="/source-data/tpch/"
target_dir_base="/data/tpch/"

# Loop over all combinations of cluster algorithms, cost models, and layering orders
for scale_factor in "${scale_factors[@]}"; do
    data_dir="${source_dir_base}/sf${scale_factor}"
    target_dir="${target_dir_base}/sf${scale_factor}"
    
    rm -rf ${target_dir}
    mkdir -p ${target_dir}
    
    ${BUILD_DIR}/store 365GB &
    ${BUILD_DIR}/catalog_server ${target_dir} &
    sleep 10

    ${BUILD_DIR}/bulkload \
        --output_dir ${target_dir} \
        --nodes LINEITEM ${data_dir}/lineitem.tbl \
        --nodes ORDERS ${data_dir}/orders.tbl \
        --nodes CUSTOMER ${data_dir}/customer.tbl \
        --nodes NATION ${data_dir}/nation.tbl \
        --nodes REGION ${data_dir}/region.tbl \
        --nodes PART ${data_dir}/part.tbl \
        --nodes SUPPLIER ${data_dir}/supplier.tbl \
        --relationships CUST_BELONG_TO ${data_dir}/customer_belongTo_nation.tbl \
        --relationships_backward CUST_BELONG_TO ${data_dir}/customer_belongTo_nation.tbl.backward \
        --relationships COMPOSED_BY ${data_dir}/lineitem_composedBy_part.tbl \
        --relationships_backward COMPOSED_BY ${data_dir}/lineitem_composedBy_part.tbl.backward \
        --relationships IS_PART_OF ${data_dir}/lineitem_isPartOf_orders.tbl \
        --relationships_backward IS_PART_OF ${data_dir}/lineitem_isPartOf_orders.tbl.backward \
        --relationships SUPPLIED_BY ${data_dir}/lineitem_suppliedBy_supplier.tbl \
        --relationships_backward SUPPLIED_BY ${data_dir}/lineitem_suppliedBy_supplier.tbl.backward \
        --relationships IS_LOCATED_IN ${data_dir}/nation_isLocatedIn_region.tbl \
        --relationships_backward IS_LOCATED_IN ${data_dir}/nation_isLocatedIn_region.tbl.backward \
        --relationships MADE_BY ${data_dir}/orders_madeBy_customer.tbl \
        --relationships_backward MADE_BY ${data_dir}/orders_madeBy_customer.tbl.backward \
        --relationships SUPP_BELONG_TO ${data_dir}/supplier_belongTo_nation.tbl \
        --relationships_backward SUPP_BELONG_TO ${data_dir}/supplier_belongTo_nation.tbl.backward \
        --relationships PARTSUPP ${data_dir}/partsupp.tbl \
        --relationships_backward PARTSUPP ${data_dir}/partsupp.tbl.backward \
        --relationships BELONG_TO_SAME_ORDER ${data_dir}/part_belong_to_same_order_part.tbl \
        --relationships PURCHASE_SAME_ITEM ${data_dir}/customer_purchase_same_item_customer.tbl \
        --relationships BOUGHT ${data_dir}/customer_bought_item.tbl \

    pkill -f store
    pkill -f catalog_server
    sleep 5
done
