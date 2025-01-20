MATCH (li:LINEITEM)-[:SUPPLIED_BY]->(s:SUPPLIER)-[:SUPP_BELONG_TO]->(n1:NATION)
		MATCH (li)-[:IS_PART_OF]->(o:ORDERS)-[:MADE_BY]->(c:CUSTOMER)-[:CUST_BELONG_TO]->(n2:NATION) 
		WHERE ((n1.N_NAME = 'IRAN' AND n2.N_NAME = 'ETHIOPIA')
			OR (n1.N_NAME = 'ETHIOPIA' AND n2.N_NAME = 'IRAN'))
			AND li.L_SHIPDATE >= date('1995-01-01')
			AND li.L_SHIPDATE <= date('1996-12-31')
		WITH 
			n1.N_NAME AS supp_nation,
			n2.N_NAME AS cust_nation,
			year(li.L_SHIPDATE) AS l_year,
			li.L_EXTENDEDPRICE * (1-li.L_DISCOUNT) AS volume_tmp
		RETURN 
			supp_nation,
			cust_nation,
			l_year,
			sum(volume_tmp) as volume
		ORDER BY
			supp_nation,
			cust_nation,
			l_year;