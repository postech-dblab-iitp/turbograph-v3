MATCH (l:LINEITEM)-[:IS_PART_OF]->(ord:ORDERS)-[:MADE_BY]->(c:CUSTOMER)
		WHERE c.C_MKTSEGMENT = 'HOUSEHOLD'
			AND ord.O_ORDERDATE < date('1995-03-17')
			AND l.L_SHIPDATE > date('1995-03-17')
		RETURN
			ord.O_ORDERKEY,
			sum(l.L_EXTENDEDPRICE*(1-l.L_DISCOUNT)) AS revenue,
			ord.O_ORDERDATE AS ord_date,
			ord.O_SHIPPRIORITY
		ORDER BY
			revenue DESC,
			ord_date
		LIMIT 10;