MATCH
(v1)-[:`http://xmlns.com/foaf/0.1/homepage`]->(v2), 
(v1)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v3),
(v1)-[:`http://dbpedia.org/property/clubs`]->(v5),
(v1)-[:`http://dbpedia.org/ontology/birthPlace`]->(v7)
WHERE
v3.uri = "http://dbpedia.org/ontology/SoccerPlayer" AND 
v1.`http://dbpedia.org/property/position`= "Goalkeeper" AND
v5.`http://dbpedia.org/ontology/capacity` IS NOT NULL AND
v7.`http://dbpedia.org/property/populationEstimate` IS NOT NULL
RETURN DISTINCT v1.uri AS r1, v5.`http://dbpedia.org/property/name`, v5.`http://dbpedia.org/property/nickname`, v5.`http://dbpedia.org/property/season`, v5.`http://xmlns.com/foaf/0.1/nick`, v5.`http://dbpedia.org/property/position`, v7.`http://dbpedia.org/ontology/demonym`, v7.`http://dbpedia.org/property/populationDensitySqMi`, v7.`http://dbpedia.org/property/regionalLanguages`, v7.`http://dbpedia.org/property/percentWater`, v7.`http://dbpedia.org/ontology/wikiPageRevisionID`, v1.`http://dbpedia.org/ontology/birthYear`, v1.`http://dbpedia.org/ontology/wikiPageRevisionID`, v1.`http://xmlns.com/foaf/0.1/surname`, v1.`http://dbpedia.org/property/years`, v1.`http://dbpedia.org/ontology/height`