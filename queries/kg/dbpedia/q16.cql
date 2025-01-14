MATCH (v1)
WHERE v1.`http://www.w3.org/2000/01/rdf-schema#label` IS NOT NULL OR
v1.`http://dbpedia.org/ontology/abstract` IS NOT NULL OR
v1.`http://www.w3.org/2000/01/rdf-schema#comment` IS NOT NULL OR
(
    v1.`http://dbpedia.org/ontology/abstract` IS NULL AND
    v1.`http://www.w3.org/2000/01/rdf-schema#comment` IS NULL AND
    v1.`http://www.w3.org/2000/01/rdf-schema#label` IS NULL
)
RETURN DISTINCT v1.uri
LIMIT 1000