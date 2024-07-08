MATCH (n)-[outgoing:OBJECT_CAN_TRANSFORM_INTO_OBJECT]->(m)
RETURN n.id, COUNT(outgoing) AS total_outgoing_arcs;