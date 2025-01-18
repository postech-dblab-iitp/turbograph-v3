MATCH (v1)-[:`http://purl.org/dc/terms/subject`]->(v2)
WHERE v1.`http://xmlns.com/foaf/0.1/name` IS NOT NULL 
RETURN v1.uri, v2.`http://dbpedia.org/ontology/wikiPageID`, v2.`http://dbpedia.org/ontology/abstract`, v1.`http://dbpedia.org/property/faculty`, v1.`http://dbpedia.org/property/titleOrig`
