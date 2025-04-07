MATCH (v1)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v2), 
(v1)-[:`http://dbpedia.org/ontology/foundationPlace`]->(v3), 
(v4)-[:`http://dbpedia.org/ontology/developer`]->(v1), 
(v4)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v5)
RETURN v1.`http://dbpedia.org/property/name`, v1.`http://dbpedia.org/property/parent`, v1.`http://dbpedia.org/property/keyPeople`, v1.`http://dbpedia.org/ontology/extinctionDate`, v4.`http://dbpedia.org/property/recorded`, v4.`http://dbpedia.org/property/lastAlbum`, v4.`http://dbpedia.org/property/showName`, v4.`http://dbpedia.org/ontology/status`, v3.`http://www.w3.org/2000/01/rdf-schema#label`, v3.`http://dbpedia.org/property/elevationM`, v3.`http://dbpedia.org/ontology/areaUrban`, v3.`http://dbpedia.org/ontology/gridReference`