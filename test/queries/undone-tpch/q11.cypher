MATCH (p:PARTSUPP)-[:SUPPLIED_BY]->(s:SUPPLIER)-[:BELONG_TO]->(n:NATION)
WHERE n.N_NAME = 'GERMANY'
with p, sum(tofloat(p.PS_SUPPLYCOST) * tofloat(p.PS_AVAILQTY)) * 0.0001 as subquery, sum(tofloat(p.PS_SUPPLYCOST) * tofloat(p.PS_AVAILQTY)) as value 
MATCH (p:PARTSUPP)-[:SUPPLIED_BY]->(s:SUPPLIER)-[:BELONG_TO]->(n:NATION)
WHERE n.N_NAME = 'GERMANY'
	AND value > subquery
RETURN
	p.PS_PARTKEY,
	value
order by value desc;
