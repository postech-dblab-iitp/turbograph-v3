MATCH (ord: ORDERS)
OPTIONAL MATCH (ord)-[:MADE_BY]->(c: CUSTOMER)
WHERE NOT REGEXP_MATCHES(ord.O_COMMENT, '.*express.*.*requests.*')
WITH c.C_CUSTKEY AS c_id, COUNT(ord.O_ORDERKEY) AS c_count
RETURN
    c_count,
    COUNT(c_id) AS custdist
ORDER BY
    custdist DESC,
    c_count DESC;