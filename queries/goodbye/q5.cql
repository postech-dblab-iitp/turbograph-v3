MATCH (a)-[:CAN_TRANSFORM_INTO]->(b)-[r]->(c)
WHERE a.type = "B" AND c.type = "A"
RETURN COUNT(b) AS count_direct_connected_nodes;
