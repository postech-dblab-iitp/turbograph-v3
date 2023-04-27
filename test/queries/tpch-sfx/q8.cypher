MATCH (lineitem: LINEITEM)-[:COMPOSED_BY]->(:PARTSUPP)-[:COMPOSED_BY]->(p: PART)
MATCH (li:LINEITEM)-[:COMPOSED_BY]->(:PARTSUPP)-[:SUPPLIED_BY]->(s:SUPPLIER)-[:BELONG_TO]->(n2:NATION)
MATCH (li:LINEITEM)-[:IS_PART_OF]->(o:ORDER)-[:MADE_BY]->(c:CUSTOMER)-[:BELONG_TO]->(n1:NATION)
MATCH (n2:NATION)-[:IS_FROM]->(r:REGION)
MATCH (n1:NATION)-[:IS_FROM]->(r:REGION)
WHERE r.R_NAME = 'AMERICA'
	AND date(o.O_ORDERDATE) > date('1995-01-01')
	AND date(o.O_ORDERDATE) < date('1996-12-31')
	AND p.P_TYPE = 'ECONOMY ANODIZED STEEL'
WITH o, li, n2, date(o.O_ORDERDATE) as o_year, sum(li.L_EXTENDEDPRICE * (1-tofloat(li.L_DISCOUNT))) as volume, n2.N_NAME as nation
WITH o_year, volume, CASE WHEN n2.N_NAME = 'BRAZIL'
		THEN volume
		ELSE 0 END AS tmp
WITH
	distinct o_year, volume, 
	sum(tmp) AS tot
RETURN o_year, tot / volume as mkt_share
order by o_year;