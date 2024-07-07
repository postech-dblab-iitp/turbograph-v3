MATCH (n) 
WITH COUNT(n) AS total_nodes
MATCH (m)-[:OBJECT_CAN_TRANSFORM_INTO_OBJECT]->(t)
WHERE t.type = "A" 
WITH total_nodes, COUNT(m) AS target_nodes
RETURN (target_nodes * 100) / total_nodes AS percentage;
