MATCH (v1)
WHERE v1.`http://www.w3.org/2000/01/rdf-schema#label` IS NOT NULL OR
(
    v1.`http://www.w3.org/2000/01/rdf-schema#label` IS NOT NULL AND
    (v1.uri =~ 'http://dbpedia.org/resource/.*' OR
    v1.uri =~ "http://dbpedia.org/ontology/.*" OR
    v1.uri =~ "http://www.w3.org/2002/07/owl.*" OR
    v1.uri =~ "http://www.w3.org/2001/XMLSchema.*" OR
    v1.uri =~ "http://www.w3.org/2000/01/rdf-schema.*" OR
    v1.uri =~ "http://www.w3.org/1999/02/22-rdf-syntax-ns.*")
)
RETURN v1.`http://www.w3.org/2000/01/rdf-schema#label`, v1.`http://dbpedia.org/property/arrondissement`, v1.`http://dbpedia.org/property/name`, v1.`http://dbpedia.org/property/dateOfDeath`