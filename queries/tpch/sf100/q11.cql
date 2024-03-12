MATCH (pa:PART)-[p:PARTSUPP]->(s:SUPPLIER)-[:SUPP_BELONG_TO]->(n:NATION)
WHERE n.N_NAME = 'FRANCE'
WITH sum(p.PS_SUPPLYCOST * p.PS_AVAILQTY) as prev_subquery
WITH prev_subquery * 0.0001 as subquery
MATCH (pa2:PART)-[p2:PARTSUPP]->(s2:SUPPLIER)-[:SUPP_BELONG_TO]->(n2:NATION)
WHERE n2.N_NAME = 'FRANCE'
WITH pa2.P_PARTKEY AS P_PARTKEY, sum(p2.PS_SUPPLYCOST * p2.PS_AVAILQTY) as value, subquery
WHERE value > subquery
RETURN
    P_PARTKEY,
    value
ORDER BY
    value desc;