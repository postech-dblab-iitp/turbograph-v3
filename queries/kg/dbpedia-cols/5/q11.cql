MATCH (v1)-[:`http://purl.org/dc/terms/subject`]->(v2)
WHERE v1.`http://xmlns.com/foaf/0.1/name` IS NOT NULL 
RETURN v2.`http://www.w3.org/2004/02/skos/core#prefLabel`, v2.`http://dbpedia.org/ontology/abstract`, v2.`http://dbpedia.org/ontology/wikiPageID`, v2.`http://www.w3.org/2000/01/rdf-schema#comment`, v2.`http://www.w3.org/2000/01/rdf-schema#label`, v1.`http://dbpedia.org/property/twin1Country`, v1.`http://dbpedia.org/ontology/circulation`, v1.`http://dbpedia.org/property/owner`, v1.`http://dbpedia.org/property/artist`, v1.`http://dbpedia.org/ontology/escapeVelocity`
