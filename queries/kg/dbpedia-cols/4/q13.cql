MATCH (v1)-[:`http://purl.org/dc/terms/subject`]->(v2)
WHERE v1.`http://www.w3.org/2000/01/rdf-schema#label` IS NOT NULL
RETURN DISTINCT v2.uri, v2.`http://www.w3.org/2000/01/rdf-schema#comment`, v2.`http://www.w3.org/2000/01/rdf-schema#label`, v2.`http://dbpedia.org/ontology/wikiPageRevisionID`, v2.`http://dbpedia.org/ontology/wikiPageID`, v1.`http://dbpedia.org/ontology/apoapsis`, v1.`http://dbpedia.org/property/names`, v1.`http://dbpedia.org/ontology/activeYears`, v1.`http://dbpedia.org/property/owners`
