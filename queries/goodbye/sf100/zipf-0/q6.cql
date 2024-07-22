MATCH (n)-[outgoing:OBJECT_CAN_TRANSFORM_INTO_OBJECT]->(m)
RETURN n, COUNT(outgoing) AS total_outgoing_arcs;