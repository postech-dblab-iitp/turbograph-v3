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
RETURN v1.uri AS r1, v5.`http://dbpedia.org/property/nickname`, v5.`http://xmlns.com/foaf/0.1/nick`, v7.`http://dbpedia.org/ontology/longName`, v7.`http://dbpedia.org/ontology/wikiPageID`, v1.`http://dbpedia.org/ontology/wikiPageID`, v1.`http://dbpedia.org/ontology/Person/height`