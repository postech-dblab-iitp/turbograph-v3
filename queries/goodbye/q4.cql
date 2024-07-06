MATCH (n)
WITH COUNT(n) AS total_nodes
MATCH (a)-[:CAN_TRANSFORM_INTO]->(b)
WHERE b.type = "A"
WITH total_nodes, a
MATCH (a)-[r1]->(neighbor)
WITH COUNT(neighbor) AS neighbor, total_nodes
RETURN neighbor / total_nodes AS percentage;
