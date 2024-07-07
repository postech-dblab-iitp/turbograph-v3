MATCH (n)
WITH COUNT(n) AS total_nodes
MATCH (a)-[:OBJECT_CAN_TRANSFORM_INTO_OBJECT]->(b)
WHERE b.type = "A"
WITH total_nodes, a
MATCH (a)-[r1]->(neighbor)
WITH COUNT(neighbor) AS neighbors, total_nodes
RETURN neighbors / total_nodes AS percentage;
