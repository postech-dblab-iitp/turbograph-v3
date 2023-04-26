MATCH (lineitem: LINEITEM)
WITH lineitem.L_ORDERKEY as l_orderkey, SUM(lineitem.L_QUANTITY) AS sum_lquantity
WHERE sum_lquantity > 300
MATCH (item: LINEITEM)-[:IS_PART_OF]->(order: ORDER)-[:MADE_BY]->(customer: CUSTOMER)
WHERE order.id IN [l_orderkey]
RETURN
	customer.C_NAME,
	customer.id,
	order.id,
	order.O_ORDERDATE,
	order.O_TOTALPRICE,
	SUM(item.L_QUANTITY)
ORDER BY order.O_TOTALPRICE desc, order.O_ORDERDATE
LIMIT 100;