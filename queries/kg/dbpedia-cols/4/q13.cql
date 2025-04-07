MATCH (v1)-[:`http://purl.org/dc/terms/subject`]->(v2)
WHERE v1.`http://www.w3.org/2000/01/rdf-schema#label` IS NOT NULL
RETURN v2.`http://dbpedia.org/ontology/abstract`, v2.`http://www.w3.org/2000/01/rdf-schema#comment`, v2.`http://dbpedia.org/ontology/wikiPageRevisionID`, v2.`http://www.w3.org/2004/02/skos/core#prefLabel`, v1.`http://dbpedia.org/property/timezone1Dst`, v1.`http://dbpedia.org/property/cylindercount`, v1.`http://dbpedia.org/ontology/formationYear`, v1.`http://dbpedia.org/property/office`
