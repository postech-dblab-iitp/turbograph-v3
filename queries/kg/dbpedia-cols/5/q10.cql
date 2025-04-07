MATCH
(v1)-[:`http://xmlns.com/foaf/0.1/homepage`]->(v2), 
(v1)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v3),
(v1)-[:`http://dbpedia.org/property/clubs`]->(v5),
(v1)-[:`http://dbpedia.org/ontology/birthPlace`]->(v7)
WHERE
v1.`http://dbpedia.org/property/position`= "Goalkeeper" AND
v5.`http://dbpedia.org/ontology/capacity` IS NOT NULL AND
v7.`http://dbpedia.org/property/populationEstimate` IS NOT NULL
RETURN v7.`http://dbpedia.org/property/populationEstimate`, v7.`http://www.w3.org/2000/01/rdf-schema#label`, v7.`http://dbpedia.org/ontology/flag`, v7.`http://dbpedia.org/ontology/abstract`, v7.`http://dbpedia.org/ontology/demonym`, v5.`http://dbpedia.org/property/clubname`, v5.`http://dbpedia.org/ontology/abstract`, v5.`http://dbpedia.org/ontology/wikiPageRevisionID`, v5.`http://dbpedia.org/property/name`, v5.`http://dbpedia.org/property/season`, v1.`http://dbpedia.org/ontology/birthDate`, v1.`http://dbpedia.org/ontology/Person/height`, v1.`http://dbpedia.org/property/nationalgoals`, v1.`http://dbpedia.org/property/position`, v1.`http://dbpedia.org/ontology/wikiPageRevisionID`