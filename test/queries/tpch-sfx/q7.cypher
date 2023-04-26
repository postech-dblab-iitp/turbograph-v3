MATCH (li:LINEITEM)-[:SUPPLIED_BY]->(s:SUPPLIER)-[:BELONG_TO]->(n1:NATION)
MATCH (li:LINEITEM)-[:IS_PART_OF]->(o:ORDER)-[:MADE_BY]->(c:CUSTOMER)-[:BELONG_TO]->(n2:NATION) 
WHERE (n1.N_NAME = 'FRANCE'and n2.N_NAME = 'GERMANY')
	or (n1.N_NAME = 'GERMANY' and n2.N_NAME = 'FRANCE')
	AND date(li.L_SHIPDATE) > date('1995-01-01')
	AND date(li.L_SHIPDATE) < date('1996-12-31')
RETURN
	n1.N_NAME as supp_nation,
	n2.N_NAME as cust_nation,
	date(li.L_SHIPDATE).year as l_year,
	sum(li.L_EXTENDEDPRICE * (1-tofloat(li.L_DISCOUNT))) as volume
order by supp_nation, cust_nation, l_year;