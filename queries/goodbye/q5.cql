MATCH (a)-[:OBJECT_CAN_TRANSFORM_INTO_OBJECT]->(b)-[r]->(c)
WHERE a.type = "B" AND c.type = "A"
RETURN COUNT(b) AS count_direct_connected_nodes;
