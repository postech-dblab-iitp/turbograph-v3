#!/bin/bash

./tbgpp-execution-engine/demo_main \
	--nodes:LINEITEM /source-data/tpch/sf1/lineitem.tbl.woadj \
	--nodes:ORDERS /source-data/tpch/sf1/orders.tbl.woadj \
	--relationships:IS_PART_OF /source-data/tpch/sf1/lineitem_isPartOf_orders.tbl
