MATCH (order: ORDER)
OPTIONAL MATCH (order: ORDER)-[:MADE_BY]->(customer: CUSTOMER)
WHERE NOT (order.O_COMMENT =~ '.*special.*.*requests.*')
WITH customer.id AS c_id, COUNT(order.id) AS c_count 
RETURN
	c_count,
	COUNT(c_id) AS custdist
ORDER BY custdist DESC, c_count DESC;