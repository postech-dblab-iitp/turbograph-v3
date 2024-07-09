MATCH (a)-[:OBJECT_CAN_TRANSFORM_INTO_OBJECT]->(b)-[r:OBJECT_CAN_TRANSFORM_INTO_OBJECT]->(c)
WHERE a.type = "B" AND c.type = "A"
RETURN a.type, c.type, COUNT(b) AS count_direct_connected_nodes;