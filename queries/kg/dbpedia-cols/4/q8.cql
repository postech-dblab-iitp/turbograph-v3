MATCH (v1)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v2), 
(v1)-[:`http://dbpedia.org/ontology/nationality`]->(v3)
WHERE v2.uri = "http://dbpedia.org/ontology/Person" AND 
v3.`http://www.w3.org/2000/01/rdf-schema#label` IS NOT NULL AND 
v1.`http://www.w3.org/2000/01/rdf-schema#label` IS NOT NULL
RETURN DISTINCT v3.`http://dbpedia.org/property/vorwahl`, v3.`http://dbpedia.org/ontology/gridReference`, v3.`http://dbpedia.org/ontology/postalCode`, v3.`http://dbpedia.org/property/populationDemonym`, v1.`http://dbpedia.org/ontology/alias`, v1.`http://dbpedia.org/property/elevationMaxM`, v1.`http://www.w3.org/2000/01/rdf-schema#label`, v1.`http://dbpedia.org/property/eyeColor`