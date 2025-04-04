MATCH (v1)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v2), 
(v1)-[:`http://dbpedia.org/ontology/foundationPlace`]->(v3), 
(v4)-[:`http://dbpedia.org/ontology/developer`]->(v1), 
(v4)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v5)
WHERE v2.uri = "http://dbpedia.org/ontology/Organisation"
RETURN v4.uri AS r1, v5.uri AS r2, v3.`http://dbpedia.org/property/percentWater`, v3.`http://dbpedia.org/property/type`, v3.`http://dbpedia.org/property/licence`, v4.`http://dbpedia.org/property/artist`, v4.`http://dbpedia.org/property/cover`, v4.`http://www.w3.org/2000/01/rdf-schema#label`, v1.`http://dbpedia.org/property/equity`, v1.`http://dbpedia.org/property/locationCity`, v1.`http://dbpedia.org/ontology/foundingDate`