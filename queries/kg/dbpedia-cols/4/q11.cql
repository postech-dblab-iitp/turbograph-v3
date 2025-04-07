MATCH (v1)-[:`http://purl.org/dc/terms/subject`]->(v2)
WHERE v1.`http://xmlns.com/foaf/0.1/name` IS NOT NULL 
RETURN v2.`http://dbpedia.org/ontology/wikiPageID`, v2.`http://www.w3.org/2004/02/skos/core#prefLabel`, v2.`http://www.w3.org/2000/01/rdf-schema#label`, v2.`http://dbpedia.org/ontology/wikiPageRevisionID`, v1.`http://dbpedia.org/property/lastAired`, v1.`http://dbpedia.org/property/wheelbase`, v1.`http://dbpedia.org/property/location`, v1.`http://dbpedia.org/ontology/Planet/meanTemperature`
