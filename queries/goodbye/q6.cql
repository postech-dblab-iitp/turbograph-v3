MATCH (n)-[outgoing]->(m)
RETURN n, COUNT(outgoing) AS total_outgoing_arcs;