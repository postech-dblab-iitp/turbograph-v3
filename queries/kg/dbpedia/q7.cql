MATCH (v1)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v2), 
(v1)-[:`http://xmlns.com/foaf/0.1/homepage`]->(v4)
WHERE v2.uri = "http://dbpedia.org/class/yago/Company108058098" AND
v1.`http://dbpedia.org/property/numEmployees` IS NOT NULL
RETURN DISTINCT v1.uri AS r1
LIMIT 1000
