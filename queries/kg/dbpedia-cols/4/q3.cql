MATCH (v1)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v2), 
(v1)-[:`http://dbpedia.org/property/website`]->(v4)
WHERE v2.uri = "http://dbpedia.org/ontology/Person" AND v1.`http://www.w3.org/2000/01/rdf-schema#label` IS NOT NULL
RETURN v1.`http://www.w3.org/2000/01/rdf-schema#label` AS r1, v4.`http://www.w3.org/2000/01/rdf-schema#comment`, v4.`http://dbpedia.org/ontology/foundingYear`, v4.`http://dbpedia.org/ontology/abstract`, v4.`http://www.w3.org/2003/01/geo/wgs84_pos#long`, v1.`http://dbpedia.org/property/period`, v1.`http://dbpedia.org/property/resides`, v1.`http://dbpedia.org/ontology/formerName`, v1.`http://www.w3.org/2003/01/geo/wgs84_pos#long`
