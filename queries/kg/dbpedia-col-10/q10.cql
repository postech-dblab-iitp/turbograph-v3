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
RETURN DISTINCT v1.uri AS r1, v5.`http://dbpedia.org/ontology/formationDate`, v5.`http://dbpedia.org/property/season`, v5.`http://www.w3.org/2000/01/rdf-schema#label`, v5.`http://dbpedia.org/property/manager`, v5.`http://dbpedia.org/ontology/wikiPageID`, v5.`http://dbpedia.org/property/owner`, v5.`http://xmlns.com/foaf/0.1/nick`, v5.`http://dbpedia.org/property/founded`, v5.`http://dbpedia.org/ontology/abstract`, v5.`http://dbpedia.org/property/capacity`, v7.`http://dbpedia.org/property/populationDensityKm`, v7.`http://dbpedia.org/property/largestCity`, v7.`http://dbpedia.org/property/conventionalLongName`, v7.`http://dbpedia.org/property/areaSqMi`, v7.`http://dbpedia.org/ontology/flag`, v7.`http://dbpedia.org/ontology/populationTotal`, v7.`http://dbpedia.org/property/establishedDate`, v7.`http://dbpedia.org/property/populationDensitySqMi`, v7.`http://dbpedia.org/ontology/percentageOfAreaWater`, v7.`http://dbpedia.org/ontology/PopulatedPlace/populationDensity`, v1.`http://dbpedia.org/ontology/wikiPageID`, v1.`http://dbpedia.org/property/name`, v1.`http://dbpedia.org/property/placeOfBirth`, v1.`http://www.w3.org/2000/01/rdf-schema#label`, v1.`http://dbpedia.org/ontology/Person/height`, v1.`http://dbpedia.org/property/position`, v1.`http://dbpedia.org/property/dateOfBirth`, v1.`http://dbpedia.org/property/goals`, v1.`http://dbpedia.org/ontology/abstract`, v1.`http://dbpedia.org/property/years`