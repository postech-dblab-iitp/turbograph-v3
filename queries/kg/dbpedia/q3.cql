MATCH (v1)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v2), 
(v1)-[:`http://dbpedia.org/property/website`]->(v4)
WHERE v2.uri = "http://dbpedia.org/ontology/Person" AND v1.`http://www.w3.org/2000/01/rdf-schema#label` IS NOT NULL
RETURN DISTINCT v1.`http://www.w3.org/2000/01/rdf-schema#label` AS r1
LIMIT 1000
