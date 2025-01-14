MATCH (v1)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v2)
WHERE v1.`http://dbpedia.org/property/isbn` IS NOT NULL AND
v1.`http://dbpedia.org/property/name` IS NOT NULL AND
v1.`http://dbpedia.org/property/pages` IS NOT NULL AND
v1.`http://dbpedia.org/property/author` IS NOT NULL
RETURN DISTINCT v2.uri AS r1
LIMIT 1000
