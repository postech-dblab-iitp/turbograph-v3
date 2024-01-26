MATCH (li:LINEITEM)-[:COMPOSED_BY]->(:PARTSUPP)-[:SUPPLIED_BY]->(s:SUPPLIER)
MATCH (ps:PARTSUPP)-[:SUPPLIED_BY]->(s:SUPPLIER)
MATCH (ps:PARTSUPP)-[:COMPOSED_BY]->(p:PART)
MATCH (s:SUPPLIER)-[:BELONG_TO]->(n:NATION)
MATCH (li:LINEITEM)-[:IS_PART_OF]->(o:ORDER)
where p.P_NAME contains 'green'
with li,ps,n,o 
order by n.N_NAME desc, date(o.O_ORDERDATE).year
return
	n.N_NAME as nation,
	date(o.O_ORDERDATE).year as year,
	sum(tofloat(li.L_EXTENDEDPRICE) * (1 -tofloat(li.L_DISCOUNT))-ps.PS_SUPPLYCOST * li.L_QUANTITY) as amount;