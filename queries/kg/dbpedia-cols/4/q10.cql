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
RETURN v1.uri AS r1, v5.`http://dbpedia.org/property/season`, v5.`http://dbpedia.org/ontology/wikiPageRevisionID`, v5.`http://dbpedia.org/property/clubname`, v5.`http://dbpedia.org/ontology/capacity`, v7.`http://dbpedia.org/ontology/wikiPageID`, v7.`http://dbpedia.org/property/areaKm`, v7.`http://www.w3.org/2003/01/geo/wgs84_pos#lat`, v7.`http://dbpedia.org/property/largestCity`, v1.`http://dbpedia.org/property/name`, v1.`http://dbpedia.org/ontology/number`, v1.`http://dbpedia.org/property/years`, v1.`http://dbpedia.org/ontology/Person/height`