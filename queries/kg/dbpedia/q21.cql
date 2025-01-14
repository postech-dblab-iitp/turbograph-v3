MATCH 
(v6)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v4)
WHERE
v4.uri = "http://dbpedia.org/ontology/Place" AND
v6.`http://www.w3.org/2003/01/geo/wgs84_pos#lat` IS NOT NULL AND
v6.`http://www.w3.org/2003/01/geo/wgs84_pos#long` IS NOT NULL AND
v6.`http://www.w3.org/2000/01/rdf-schema#label` IS NOT NULL AND
v6.`http://dbpedia.org/ontology/abstract` IS NOT NULL
WITH v6
OPTIONAL MATCH (v6)-[:`http://xmlns.com/foaf/0.1/depiction`]->(v8)
OPTIONAL MATCH (v6)-[:`http://xmlns.com/foaf/0.1/homepage`]->(v10)
OPTIONAL MATCH (v6)-[:`http://dbpedia.org/ontology/thumbnail`]->(v14)
RETURN DISTINCT v6.`http://www.w3.org/2000/01/rdf-schema#label` AS r1
LIMIT 1000
