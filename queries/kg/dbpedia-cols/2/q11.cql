MATCH (v1)-[:`http://purl.org/dc/terms/subject`]->(v2)
WHERE v1.`http://xmlns.com/foaf/0.1/name` IS NOT NULL 
RETURN v2.`http://www.w3.org/2000/01/rdf-schema#label`, v2.`http://dbpedia.org/ontology/wikiPageRevisionID`, v1.`http://dbpedia.org/property/profession`, v1.`http://dbpedia.org/property/absMagnitude`
