MATCH (v1)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v2)
RETURN DISTINCT v1.uri AS r1
LIMIT 1000
