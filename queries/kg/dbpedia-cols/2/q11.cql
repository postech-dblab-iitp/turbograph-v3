MATCH (v1)-[:`http://purl.org/dc/terms/subject`]->(v2)
WHERE v1.`http://xmlns.com/foaf/0.1/name` IS NOT NULL 
RETURN v1.uri, v2.`http://dbpedia.org/ontology/wikiPageID`, v2.`http://www.w3.org/2004/02/skos/core#prefLabel`, v1.`http://dbpedia.org/property/dischargeMaxImperial`, v1.`http://dbpedia.org/property/nativename`
