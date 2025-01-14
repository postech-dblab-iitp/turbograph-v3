MATCH (v1)
OPTIONAL MATCH (v1)-[:`http://xmlns.com/foaf/0.1/depiction`]->(v2)
OPTIONAL MATCH (v1)-[:`http://xmlns.com/foaf/0.1/homepage`]->(v3)
WHERE
v1.`http://www.w3.org/2000/01/rdf-schema#comment` IS NOT NULL
RETURN DISTINCT v1.uri
LIMIT 1000