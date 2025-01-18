MATCH (v1)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v2)
WHERE v1.`http://dbpedia.org/property/isbn` IS NOT NULL AND
v1.`http://dbpedia.org/property/name` IS NOT NULL AND
v1.`http://dbpedia.org/property/pages` IS NOT NULL AND
v1.`http://dbpedia.org/property/author` IS NOT NULL
RETURN v2.uri AS r1, v1.`http://dbpedia.org/property/pictureFormat`, v1.`http://dbpedia.org/ontology/numberOfEpisodes`, v1.`http://dbpedia.org/property/genre`, v1.`http://dbpedia.org/ontology/releaseDate`, v1.`http://dbpedia.org/property/showName`, v1.`http://dbpedia.org/property/language`, v1.`http://dbpedia.org/property/precededBy`, v1.`http://dbpedia.org/ontology/Work/runtime`, v1.`http://dbpedia.org/property/mediaType`, v1.`http://www.w3.org/2000/01/rdf-schema#comment`
