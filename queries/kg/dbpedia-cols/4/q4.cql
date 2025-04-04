MATCH (v1)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v2), 
(v1)-[:`http://dbpedia.org/ontology/foundationPlace`]->(v3), 
(v4)-[:`http://dbpedia.org/ontology/developer`]->(v1), 
(v4)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v5)
WHERE v2.uri = "http://dbpedia.org/ontology/Organisation"
RETURN v4.uri AS r1, v5.uri AS r2, v3.`http://dbpedia.org/property/utcOffsetDst`, v3.`http://dbpedia.org/ontology/postalCode`, v3.`http://dbpedia.org/property/populationTotal`, v3.`http://dbpedia.org/ontology/elevation`, v4.`http://dbpedia.org/property/filename`, v4.`http://dbpedia.org/property/champion`, v4.`http://dbpedia.org/property/published`, v4.`http://dbpedia.org/ontology/wikiPageID`, v1.`http://www.georss.org/georss/point`, v1.`http://dbpedia.org/property/equity`, v1.`http://dbpedia.org/ontology/foundingDate`, v1.`http://dbpedia.org/ontology/fate`