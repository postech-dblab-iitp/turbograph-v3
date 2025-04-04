MATCH (v1)-[:`http://purl.org/dc/terms/subject`]->(v2)
WHERE v1.`http://xmlns.com/foaf/0.1/name` IS NOT NULL 
RETURN v1.uri, v2.`http://www.w3.org/2000/01/rdf-schema#label`, v2.`http://dbpedia.org/ontology/wikiPageID`, v2.`http://dbpedia.org/ontology/abstract`, v2.`http://dbpedia.org/ontology/wikiPageRevisionID`, v1.`http://www.w3.org/2003/01/geo/wgs84_pos#long`, v1.`http://dbpedia.org/ontology/distanceToLondon`, v1.`http://dbpedia.org/property/populationUrban`, v1.`http://dbpedia.org/ontology/iataAirlineCode`
