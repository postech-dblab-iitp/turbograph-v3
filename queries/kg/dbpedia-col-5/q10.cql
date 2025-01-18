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
RETURN DISTINCT v1.uri AS r1, v5.`http://dbpedia.org/property/season`, v5.`http://dbpedia.org/ontology/abstract`, v5.`http://dbpedia.org/property/founded`, v5.`http://dbpedia.org/property/fullname`, v5.`http://www.w3.org/2000/01/rdf-schema#label`, v1.`http://dbpedia.org/ontology/abstract`, v1.`http://dbpedia.org/property/position`, v1.`http://dbpedia.org/property/years`, v1.`http://dbpedia.org/ontology/Person/height`, v1.`http://dbpedia.org/property/nationalyears`, v7.`http://dbpedia.org/property/populationDensitySqMi`, v7.`http://dbpedia.org/ontology/demonym`, v7.`http://dbpedia.org/property/commonName`, v7.`http://www.w3.org/2000/01/rdf-schema#label`, v7.`http://www.georss.org/georss/point`