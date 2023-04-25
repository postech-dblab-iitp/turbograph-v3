MATCH (lineitem: LINEITEM)-[:IS_PART_OF]->(order: ORDER)
WHERE
	date(lineitem.L_COMMITDATE) < date(lineitem.L_RECEIPTDATE)
	AND date(order.O_ORDERDATE) >= date('1993-07-01')
	AND date(order.O_ORDERDATE) < date(date('1993-07-01') + duration('P3M'))
RETURN
	order.O_ORDERPRIORITY, COUNT(*) AS ORDER_COUNT
ORDER BY order.O_ORDERPRIORITY;