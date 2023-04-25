MATCH (lineitem: LINEITEM)-[:IS_PART_OF]->(order: ORDER)-[:MADE_BY]->(customer: CUSTOMER)-[:BELONG_TO]->(nation: NATION)-[:IS_FROM]-(region: REGION)
WHERE region.R_NAME = 'ASIA'
	AND date(order.O_ORDERDATE) >= date('1994-01-01')
	AND date(order.O_ORDERDATE) < date(date('1994-01-01') + duration('P1Y'))
RETURN
	nation.N_NAME,
	SUM(lineitem.L_EXTENDEDPRICE * (1 - lineitem.L_DISCOUNT)) AS REVENUE
ORDER BY REVENUE DESC;