MATCH (p:PART)<-[:COMPOSED_BY]-(li:LINEITEM)-[:SUPPLIED_BY]->(s:SUPPLIER),
			(p)-[ps:PARTSUPP]->(s),
			(s)-[:SUPP_BELONG_TO]->(n:NATION),
			(li)-[:IS_PART_OF]->(o:ORDERS)
		WHERE p.P_NAME CONTAINS 'ivory'
		WITH n.N_NAME as nation, year(o.O_ORDERDATE) as year, li.L_EXTENDEDPRICE * (1 -li.L_DISCOUNT) - ps.PS_SUPPLYCOST * li.L_QUANTITY as tmp
		RETURN
			nation,
			year, 
			sum(tmp) as amount
		ORDER BY nation, year desc;