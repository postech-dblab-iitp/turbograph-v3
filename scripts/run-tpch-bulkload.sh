#!/bin/bash

SUFFIX=""
SF=1

./tbgpp-execution-engine/bulkload_using_map2 \
	--output_dir:"/data/tpch/sf${SF}/" \
	--nodes:LINEITEM /source-data/tpch/sf${SF}/lineitem.tbl.woadj${SUFFIX} \
	--nodes:ORDERS /source-data/tpch/sf${SF}/orders.tbl.woadj${SUFFIX} \
	--nodes:CUSTOMER /source-data/tpch/sf${SF}/customer.tbl.woadj${SUFFIX} \
	--nodes:NATION /source-data/tpch/sf${SF}/nation.tbl.woadj${SUFFIX} \
	--relationships:IS_PART_OF /source-data/tpch/sf${SF}/lineitem_isPartOf_orders.tbl${SUFFIX} \
	--relationships:IS_PART_OF_BACKWARD /source-data/tpch/sf${SF}/lineitem_isPartOf_orders.tbl${SUFFIX}.backward \
	--relationships:MADE_BY /source-data/tpch/sf${SF}/orders_madeBy_customer.tbl${SUFFIX} \
	--relationships:BELONG_TO /source-data/tpch/sf${SF}/customer_belongTo_nation.tbl${SUFFIX}
exit

./tbgpp-execution-engine/bulkload_using_index \
	--output_dir:"/data/test/" \
	--nodes:LINEITEM /source-data/tpch/sf${SF}/lineitem.tbl.woadj${SUFFIX} \
	--nodes:ORDERS /source-data/tpch/sf${SF}/orders.tbl.woadj${SUFFIX} \
	--nodes:CUSTOMER /source-data/tpch/sf${SF}/customer.tbl.woadj${SUFFIX} \
	--nodes:NATION /source-data/tpch/sf${SF}/nation.tbl.woadj${SUFFIX} \
	--relationships:IS_PART_OF /source-data/tpch/sf${SF}/lineitem_isPartOf_orders.tbl${SUFFIX} \
	--relationships:IS_PART_OF_BACKWARD /source-data/tpch/sf${SF}/lineitem_isPartOf_orders.tbl${SUFFIX}.backward \
	--relationships:MADE_BY /source-data/tpch/sf${SF}/orders_madeBy_customer.tbl${SUFFIX} \
	--relationships:BELONG_TO /source-data/tpch/sf${SF}/customer_belongTo_nation.tbl${SUFFIX}
