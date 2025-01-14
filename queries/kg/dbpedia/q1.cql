MATCH (v1)-[:`http://dbpedia.org/ontology/wikiPageRedirects`]->(v2) RETURN DISTINCT v1.uri AS r1 LIMIT 1000
