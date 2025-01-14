MATCH (v1)-[:`http://dbpedia.org/property/series`]->(v4),
(v4)-[:`http://dbpedia.org/ontology/wikiPageRedirects`]->(v2),
(v1)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v3)
WHERE 
v1.`http://xmlns.com/foaf/0.1/name` IS NOT NULL AND
v1.`http://www.w3.org/2000/01/rdf-schema#comment` IS NOT NULL
RETURN DISTINCT v3.uri, v2.uri
LIMIT 1000