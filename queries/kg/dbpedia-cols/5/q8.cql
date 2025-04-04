MATCH (v1)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v2), 
(v1)-[:`http://dbpedia.org/ontology/nationality`]->(v3)
WHERE v2.uri = "http://dbpedia.org/ontology/Person" AND 
v3.`http://www.w3.org/2000/01/rdf-schema#label` IS NOT NULL AND 
v1.`http://www.w3.org/2000/01/rdf-schema#label` IS NOT NULL
RETURN DISTINCT v1.`http://www.w3.org/2000/01/rdf-schema#label` AS r1, v3.`http://dbpedia.org/property/fullname`, v3.`http://dbpedia.org/property/composer`, v3.`http://dbpedia.org/ontology/areaMetro`, v3.`http://dbpedia.org/property/utcOffset1Dst`, v3.`http://dbpedia.org/ontology/PopulatedPlace/populationMetroDensity`, v1.`http://dbpedia.org/property/weight`, v1.`http://dbpedia.org/property/birthdate`, v1.`http://dbpedia.org/ontology/eyeColor`, v1.`http://dbpedia.org/ontology/draft`, v1.`http://dbpedia.org/property/birthname`