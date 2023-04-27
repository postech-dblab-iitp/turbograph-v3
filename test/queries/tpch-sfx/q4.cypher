MATCH (order:ORDER)
WHERE
	date(order.O_ORDERDATE) >= date('1993-01-01')
	AND date(order.O_ORDERDATE) < date('1993-01-01') + duration('P3M')
	AND EXISTS {
		MATCH (lineitem:LINEITEM)-[:IS_PART_OF]->(order)
		WHERE lineitem.L_COMMITDATE < lineitem.L_RECEIPTDATE
	}
RETURN 
	order.ORDERPRIORITY,
	count(*) AS ORDER_COUNT
ORDER BY order.ORDERPRIORITY;