#!/bin/bash

basedir=$1

# Remove last character '|' for each line
sed 's/.$//' ${basedir}/customer.tbl.original > ${basedir}/customer.tbl
sed 's/.$//' ${basedir}/lineitem.tbl.original > ${basedir}/lineitem.tbl
sed 's/.$//' ${basedir}/nation.tbl.original > ${basedir}/nation.tbl
sed 's/.$//' ${basedir}/orders.tbl.original > ${basedir}/orders.tbl
sed 's/.$//' ${basedir}/part.tbl.original > ${basedir}/part.tbl
sed 's/.$//' ${basedir}/region.tbl.original > ${basedir}/region.tbl
sed 's/.$//' ${basedir}/supplier.tbl.original > ${basedir}/supplier.tbl
sed 's/.$//' ${basedir}/partsupp.tbl.original > ${basedir}/partsupp.tbl

# Insert header
list=(
"customer C_CUSTKEY:ID(CUSTOMER)|C_NAME:STRING|C_ADDRESS:STRING|C_NATIONKEY:ADJLIST(NATION)|C_PHONE:STRING|C_ACCTBAL:DECIMAL(12,2)|C_MKTSEGMENT:STRING|C_COMMENT:STRING"
"lineitem L_ORDERKEY:ID_1(LINEITEM)|L_PARTKEY:ADJLIST(PART)|L_SUPPKEY:ADJLIST(SUPPLIER)|L_LINENUMBER:ID_2(LINEITEM)|L_QUANTITY:INT|L_EXTENDEDPRICE:DECIMAL(12,2)|L_DISCOUNT:DECIMAL(12,2)|L_TAX:DECIMAL(12,2)|L_RETURNFLAG:STRING|L_LINESTATUS:STRING|L_SHIPDATE:DATE|L_COMMITDATE:DATE|L_RECEIPTDATE:DATE|L_SHIPINSTRUCT:STRING|L_SHIPMODE:STRING|L_COMMENT:STRING"
"nation N_NATIONKEY:ID(NATION)|N_NAME:STRING|N_REGIONKEY:ADJLIST(REGION)|N_COMMENT:STRING"
"orders O_ORDERKEY:ID(ORDERS)|O_CUSTKEY:ADJLIST(CUSTOMER)|O_ORDERSTATUS:STRING|O_TOTALPRICE:DECIMAL(12,2)|O_ORDERDATE:DATE|O_ORDERPRIORITY:STRING|O_CLERK:STRING|O_SHIPPRIORITY:INT|O_COMMENT:STRING"
"part P_PARTKEY:ID(PART)|P_NAME:STRING|P_MFGR:STRING|P_BRAND:STRING|P_TYPE:STRING|P_SIZE:INT|P_CONTAINER:STRING|P_RETAILPRICE:DECIMAL(12,2)|P_COMMENT:STRING"
"region R_REGIONKEY:ID(REGION)|R_NAME:STRING|R_COMMENT:STRING"
"supplier S_SUPPKEY:ID(SUPPLIER)|S_NAME:STRING|S_ADDRESS:STRING|S_NATIONKEY:ADJLIST(NATION)|S_PHONE:STRING|S_ACCTBAL:DECIMAL(12,2)|S_COMMENT:STRING"
"partsupp :START_ID(PART)|:END_ID(SUPPLIER)|PS_AVAILQTY:INT|PS_SUPPLYCOST:DECIMAL(12,2)|PS_COMMENT:STRING"
)

for ((i = 0; i < ${#list[@]}; i++)); do
        IFS=' ' read -ra array <<< "${list[$i]}"
        sed -i '1s/^/'${array[1]}'\n/' "${basedir}/${array[0]}.tbl"
done

# Remove adjlist column
cat ${basedir}/customer.tbl | awk -F '|' '{print $1"|"$2"|"$3"|"$5"|"$6"|"$7"|"$8}' > ${basedir}/customer.tbl.woadj
cat ${basedir}/lineitem.tbl | awk -F '|' '{print $1"|"$4"|"$5"|"$6"|"$7"|"$8"|"$9"|"$10"|"$11"|"$12"|"$13"|"$14"|"$15"|"$16}' > ${basedir}/lineitem.tbl.woadj
cat ${basedir}/nation.tbl | awk -F '|' '{print $1"|"$2"|"$4}' > ${basedir}/nation.tbl.woadj
cat ${basedir}/orders.tbl | awk -F '|' '{print $1"|"$3"|"$4"|"$5"|"$6"|"$7"|"$8"|"$9}' > ${basedir}/orders.tbl.woadj
cp ${basedir}/part.tbl ${basedir}/part.tbl.woadj
cp ${basedir}/region.tbl ${basedir}/region.tbl.woadj
cat ${basedir}/supplier.tbl | awk -F '|' '{print $1"|"$2"|"$3"|"$5"|"$6"|"$7}' > ${basedir}/supplier.tbl.woadj

# Generate forward edge data
tail -n+2 ${basedir}/customer.tbl | awk -F '|' '{print $1"|"$4}' > ${basedir}/customer_belongTo_nation.tbl
tail -n+2 ${basedir}/lineitem.tbl | awk -F '|' '{print $1"|"$4"|"$1}' > ${basedir}/lineitem_isPartOf_orders.tbl
tail -n+2 ${basedir}/lineitem.tbl | awk -F '|' '{print $1"|"$4"|"$2}' > ${basedir}/lineitem_composedBy_part.tbl
tail -n+2 ${basedir}/lineitem.tbl | awk -F '|' '{print $1"|"$4"|"$3}' > ${basedir}/lineitem_suppliedBy_supplier.tbl
tail -n+2 ${basedir}/nation.tbl | awk -F '|' '{print $1"|"$3}' > ${basedir}/nation_isLocatedIn_region.tbl
tail -n+2 ${basedir}/orders.tbl | awk -F '|' '{print $1"|"$2}' > ${basedir}/orders_madeBy_customer.tbl
tail -n+2 ${basedir}/supplier.tbl | awk -F '|' '{print $1"|"$4}' > ${basedir}/supplier_belongTo_nation.tbl

# Generate backward edge data
cat ${basedir}/customer_belongTo_nation.tbl | sort -t '|' -n -k 2 -k 1 | awk -F '|' '{print $2"|"$1}' > ${basedir}/customer_belongTo_nation.tbl.backward
cat ${basedir}/lineitem_isPartOf_orders.tbl | sort -t '|' -n -k 3 -k 1 -k 2 | awk -F '|' '{print $3"|"$1"|"$2}' > ${basedir}/lineitem_isPartOf_orders.tbl.backward
cat ${basedir}/lineitem_composedBy_part.tbl | sort -t '|' -n -k 3 -k 1 -k 2 | awk -F '|' '{print $3"|"$1"|"$2}' > ${basedir}/lineitem_composedBy_part.tbl.backward
cat ${basedir}/lineitem_suppliedBy_supplier.tbl | sort -t '|' -n -k 3 -k 1 -k 2 | awk -F '|' '{print $3"|"$1"|"$2}' > ${basedir}/lineitem_suppliedBy_supplier.tbl.backward
cat ${basedir}/nation_isLocatedIn_region.tbl | sort -t '|' -n -k 2 -k 1 | awk -F '|' '{print $2"|"$1}' > ${basedir}/nation_isLocatedIn_region.tbl.backward
cat ${basedir}/orders_madeBy_customer.tbl | sort -t '|' -n -k 2 -k 1 | awk -F '|' '{print $2"|"$1}' > ${basedir}/orders_madeBy_customer.tbl.backward
cat ${basedir}/supplier_belongTo_nation.tbl | sort -t '|' -n -k 2 -k 1 | awk -F '|' '{print $2"|"$1}' > ${basedir}/supplier_belongTo_nation.tbl.backward
cat ${basedir}/partsupp.tbl | sort -t '|' -n -k 2 -k 1 | awk -F '|' '{print $2"|"$1"|"$3"|"$4"|"$5}' > ${basedir}/partsupp.tbl.backward

# Insert edge header
edge_list=(
"customer_belongTo_nation :START_ID(CUSTOMER)|:END_ID(NATION)"
"lineitem_isPartOf_orders :START_ID_1(LINEITEM)|:START_ID_2(LINEITEM)|:END_ID(ORDERS)"
"lineitem_composedBy_part :START_ID_1(LINEITEM)|:START_ID_2(LINEITEM)|:END_ID(PART)"
"lineitem_suppliedBy_supplier :START_ID_1(LINEITEM)|:START_ID_2(LINEITEM)|:END_ID(SUPPLIER)"
"nation_isLocatedIn_region :START_ID(NATION)|:END_ID(REGION)"
"orders_madeBy_customer :START_ID(ORDERS)|:END_ID(CUSTOMER)"
"supplier_belongTo_nation :START_ID(SUPPLIER)|:END_ID(NATION)"
)

for ((i = 0; i < ${#edge_list[@]}; i++)); do
        IFS=' ' read -ra array <<< "${edge_list[$i]}"
        sed -i '1s/^/'${array[1]}'\n/' "${basedir}/${array[0]}.tbl"
done

bwd_edge_list=(
"customer_belongTo_nation :END_ID(NATION)|:START_ID(CUSTOMER)"
"lineitem_isPartOf_orders :END_ID(ORDERS)|:START_ID_1(LINEITEM)|:START_ID_2(LINEITEM)"
"lineitem_composedBy_part :END_ID(PART)|:START_ID_1(LINEITEM)|:START_ID_2(LINEITEM)"
"lineitem_suppliedBy_supplier :END_ID(SUPPLIER)|:START_ID_1(LINEITEM)|:START_ID_2(LINEITEM)"
"nation_isLocatedIn_region :END_ID(REGION)|:START_ID(NATION)"
"orders_madeBy_customer :END_ID(CUSTOMER)|:START_ID(ORDERS)"
"supplier_belongTo_nation :END_ID(NATION)|:START_ID(SUPPLIER)"
"partsupp :END_ID(SUPPLIER)|:START_ID(PART)|PS_AVAILQTY:INT|PS_SUPPLYCOST:DECIMAL(12,2)|PS_COMMENT:STRING"
)

for ((i = 0; i < ${#bwd_edge_list[@]}; i++)); do
        IFS=' ' read -ra array <<< "${bwd_edge_list[$i]}"
        sed -i '1s/^/'${array[1]}'\n/' "${basedir}/${array[0]}.tbl.backward"
done
