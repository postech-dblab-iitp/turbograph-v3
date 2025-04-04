MATCH (v1)-[:`http://dbpedia.org/ontology/wikiPageRedirects`]->(v2)
RETURN v1.uri AS r1, v2.`http://dbpedia.org/ontology/isoCodeRegion`, v1.`http://dbpedia.org/property/yearsactive`