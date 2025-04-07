MATCH (v1)-[:`http://www.w3.org/2002/07/owl#sameAs`]->(v2)
WHERE v2.`http://xmlns.com/foaf/0.1/name` IS NOT NULL
WITH v1
OPTIONAL MATCH (v3)-[:`http://www.w3.org/2002/07/owl#sameAs`]->(v1)
WHERE v3.`http://xmlns.com/foaf/0.1/name` IS NOT NULL
RETURN v1.uri

MATCH (v1)
WITH v1
LIMIT 100000
MATCH (v1)-[:`http://www.w3.org/2002/07/owl#sameAs`]->(v2)
RETURN v1.uri, v2.uri