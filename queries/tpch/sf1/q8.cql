MATCH (li:LINEITEM)-[:COMPOSED_BY]->(p:PART)
		MATCH (li)-[:SUPPLIED_BY]->(s:SUPPLIER)-[:SUPP_BELONG_TO]->(n2:NATION)
		MATCH (li)-[:IS_PART_OF]->(o:ORDERS)-[:MADE_BY]->(c:CUSTOMER)-[:CUST_BELONG_TO]->(n1:NATION)-[:IS_LOCATED_IN]->(r:REGION)
		WHERE r.R_NAME = 'AFRICA'
			AND o.O_ORDERDATE > date('1995-01-01')
			AND o.O_ORDERDATE < date('1996-12-31')
			AND p.P_TYPE = 'ECONOMY BRUSHED BRASS'
		WITH
			year(o.O_ORDERDATE) AS o_year,
			li.L_EXTENDEDPRICE AS L_EXTENDEDPRICE,
			li.L_DISCOUNT AS L_DISCOUNT,
			n2.N_NAME AS nation
		WITH 
			o_year,
			sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS volume,
			nation
		WITH o_year AS o_year,
			sum(CASE WHEN nation = 'ETHIOPIA'
				THEN volume
				ELSE 0 END) as total_volume,
			sum(volume) as num_volume
		RETURN o_year, total_volume / num_volume AS mkt_share
		ORDER BY o_year;