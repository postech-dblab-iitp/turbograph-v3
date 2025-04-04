MATCH (v1)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v2), 
(v1)-[:`http://dbpedia.org/property/website`]->(v4)
WHERE v2.uri = "http://dbpedia.org/ontology/Person" AND v1.`http://www.w3.org/2000/01/rdf-schema#label` IS NOT NULL
RETURN v1.`http://www.w3.org/2000/01/rdf-schema#label` AS r1, v4.`http://dbpedia.org/ontology/foundingYear`, v4.`http://xmlns.com/foaf/0.1/name`, v1.`http://dbpedia.org/property/type`, v1.`http://dbpedia.org/property/watershed`
