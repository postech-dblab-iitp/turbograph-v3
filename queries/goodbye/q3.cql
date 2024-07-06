MATCH (n) 
WITH COUNT(n) AS total_nodes
MATCH (m)-[:CAN_TRANSFORM_INTO]->(t)
WHERE t.type = "A" 
WITH total_nodes, COUNT(m) AS target_nodes
RETURN target_nodes / total_nodes AS percentage;
