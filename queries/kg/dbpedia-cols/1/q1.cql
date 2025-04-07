MATCH (v1)-[:`http://dbpedia.org/ontology/wikiPageRedirects`]->(v2)
RETURN v2.`http://dbpedia.org/property/discovered`, v1.`http://dbpedia.org/property/mainSettlement`