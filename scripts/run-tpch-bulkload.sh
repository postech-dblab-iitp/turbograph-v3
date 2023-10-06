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
	--relationships_backward:CUST_BELONG_TO /source-data/tpch/sf${SF}/customer_belongTo_nation.tbl${SUFFIX}.backward \
	--relationships:COMPOSED_BY /source-data/tpch/sf${SF}/lineitem_composedBy_part.tbl${SUFFIX} \
	--relationships_backward:COMPOSED_BY /source-data/tpch/sf${SF}/lineitem_composedBy_part.tbl${SUFFIX}.backward \
	--relationships:IS_PART_OF /source-data/tpch/sf${SF}/lineitem_isPartOf_orders.tbl${SUFFIX} \
	--relationships_backward:IS_PART_OF /source-data/tpch/sf${SF}/lineitem_isPartOf_orders.tbl${SUFFIX}.backward \
	--relationships:SUPPLIED_BY /source-data/tpch/sf${SF}/lineitem_suppliedBy_supplier.tbl${SUFFIX} \
	--relationships_backward:SUPPLIED_BY /source-data/tpch/sf${SF}/lineitem_suppliedBy_supplier.tbl${SUFFIX}.backward \
	--relationships:IS_LOCATED_IN /source-data/tpch/sf${SF}/nation_isLocatedIn_region.tbl${SUFFIX} \
	--relationships_backward:IS_LOCATED_IN /source-data/tpch/sf${SF}/nation_isLocatedIn_region.tbl${SUFFIX}.backward \
	--relationships:MADE_BY /source-data/tpch/sf${SF}/orders_madeBy_customer.tbl${SUFFIX} \
	--relationships_backward:MADE_BY /source-data/tpch/sf${SF}/orders_madeBy_customer.tbl${SUFFIX}.backward \
	--relationships:SUPP_BELONG_TO /source-data/tpch/sf${SF}/supplier_belongTo_nation.tbl${SUFFIX} \
	--relationships_backward:SUPP_BELONG_TO /source-data/tpch/sf${SF}/supplier_belongTo_nation.tbl${SUFFIX}.backward \
	--relationships:PARTSUPP /source-data/tpch/sf${SF}/partsupp.tbl \
	--relationships_backward:PARTSUPP /source-data/tpch/sf${SF}/partsupp.tbl.backward