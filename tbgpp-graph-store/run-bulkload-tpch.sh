#!/bin/bash

SCALE=1
DATA_ROOT_DIR="/source-data/tpch/"

rm -rf /data/data/*
# Run exp
build/extent_test_ldbc \
	--nodes:LINEITEM ${DATA_ROOT_DIR}/sf${SCALE}/lineitem.tbl
	#--nodes:CUSTOMER ${DATA_ROOT_DIR}/sf${SCALE}/customer.tbl
	#--nodes:NATION ${DATA_ROOT_DIR}/sf${SCALE}/nation.tbl \
	#--nodes:ORDERS ${DATA_ROOT_DIR}/sf${SCALE}/orders.tbl \
	#--nodes:PART ${DATA_ROOT_DIR}/sf${SCALE}/part.tbl \
	#--nodes:REGION ${DATA_ROOT_DIR}/sf${SCALE}/region.tbl \
	#--nodes:SUPPLIER ${DATA_ROOT_DIR}/sf${SCALE}/supplier.tbl
	#--relationships:LIKES ${DATA_ROOT_DIR}/sf${SCALE}/graphs/csv/interactive/composite-projected-fk/dynamic/Person_likes_Comment.csv \
	#--relationships_backward:LIKES ${DATA_ROOT_DIR}/sf${SCALE}/graphs/csv/interactive/composite-projected-fk/dynamic/Person_likes_Comment.csv.backward
