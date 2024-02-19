#!/bin/bash

SUFFIX=""
SF=1

./tbgpp-execution-engine/bulkload_using_map \
	--output_dir:"/data/tpch/sf${SF}/" \
	--nodes:LINEITEM /source-data/tpch/sf${SF}/lineitem.tbl.woadj${SUFFIX} \
	--nodes:ORDERS /source-data/tpch/sf${SF}/orders.tbl.woadj${SUFFIX} \
	--nodes:CUSTOMER /source-data/tpch/sf${SF}/customer.tbl.woadj${SUFFIX} \
	--nodes:NATION /source-data/tpch/sf${SF}/nation.tbl.woadj${SUFFIX} \
	--nodes:REGION /source-data/tpch/sf${SF}/region.tbl${SUFFIX} \
	--nodes:PART /source-data/tpch/sf${SF}/part.tbl.woadj${SUFFIX} \
	--nodes:SUPPLIER /source-data/tpch/sf${SF}/supplier.tbl.woadj${SUFFIX} \
	--relationships:CUST_BELONG_TO /source-data/tpch/sf${SF}/customer_belongTo_nation.tbl${SUFFIX} \
	--relationships:CUST_BELONG_TO_BWD /source-data/tpch/sf${SF}/customer_belongTo_nation.tbl${SUFFIX}.backward2 \
	--relationships:COMPOSED_BY /source-data/tpch/sf${SF}/lineitem_composedBy_part.tbl${SUFFIX} \
	--relationships:COMPOSED_BY_BWD /source-data/tpch/sf${SF}/lineitem_composedBy_part.tbl${SUFFIX}.backward2 \
	--relationships:IS_PART_OF /source-data/tpch/sf${SF}/lineitem_isPartOf_orders.tbl${SUFFIX} \
	--relationships:IS_PART_OF_BWD /source-data/tpch/sf${SF}/lineitem_isPartOf_orders.tbl${SUFFIX}.backward2 \
	--relationships:SUPPLIED_BY /source-data/tpch/sf${SF}/lineitem_suppliedBy_supplier.tbl${SUFFIX} \
	--relationships:SUPPLIED_BY_BWD /source-data/tpch/sf${SF}/lineitem_suppliedBy_supplier.tbl${SUFFIX}.backward2 \
	--relationships:IS_LOCATED_IN /source-data/tpch/sf${SF}/nation_isLocatedIn_region.tbl${SUFFIX} \
	--relationships:IS_LOCATED_IN_BWD /source-data/tpch/sf${SF}/nation_isLocatedIn_region.tbl${SUFFIX}.backward2 \
	--relationships:MADE_BY /source-data/tpch/sf${SF}/orders_madeBy_customer.tbl${SUFFIX} \
	--relationships:MADE_BY_BWD /source-data/tpch/sf${SF}/orders_madeBy_customer.tbl${SUFFIX}.backward2 \
	--relationships:SUPP_BELONG_TO /source-data/tpch/sf${SF}/supplier_belongTo_nation.tbl${SUFFIX} \
	--relationships:SUPP_BELONG_TO_BWD /source-data/tpch/sf${SF}/supplier_belongTo_nation.tbl${SUFFIX}.backward2 \
	--relationships:PARTSUPP /source-data/tpch/sf${SF}/partsupp.tbl \
	--relationships:PARTSUPP_BWD /source-data/tpch/sf${SF}/partsupp.tbl.backward2
