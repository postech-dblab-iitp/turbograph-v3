MATCH (lineitem:LINEITEM)-[:IS_PART_OF]->(order:ORDER)-[:MADE_BY]->(customer:CUSTOMER)
WHERE
	customer.C_MKTSEGMENT = 'BUILDING'
	AND date(order.O_ORDERDATE) < date('1995-03-15')
	AND date(lineitem.L_SHIPDATE) > date('1995-03-15')
RETURN
	order.id,
	sum(lineitem.L_EXTENDEDPRICE*(1-lineitem.L_DISCOUNT)) AS REVENUE,
	order.O_ORDERDATE,
	order.O_SHIPPRIORITY
ORDER BY REVENUE DESC, order.O_ORDERDATE
LIMIT 10;