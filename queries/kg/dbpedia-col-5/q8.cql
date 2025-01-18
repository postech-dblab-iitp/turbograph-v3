MATCH (v1)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v2), 
(v1)-[:`http://dbpedia.org/ontology/nationality`]->(v3)
WHERE v2.uri = "http://dbpedia.org/ontology/Person" AND 
v3.`http://www.w3.org/2000/01/rdf-schema#label` IS NOT NULL AND 
v1.`http://www.w3.org/2000/01/rdf-schema#label` IS NOT NULL
RETURN DISTINCT v1.`http://www.w3.org/2000/01/rdf-schema#label` AS r1, v1.`http://dbpedia.org/property/shoots`, v1.`http://dbpedia.org/property/clubnumber`, v1.`http://dbpedia.org/property/areaTotalKm`, v1.`http://dbpedia.org/property/eyecolor`, v1.`http://dbpedia.org/property/museum`, v3.`http://dbpedia.org/property/populationMetro`, v3.`http://dbpedia.org/property/officialLanguages`, v3.`http://dbpedia.org/property/largestCity`, v3.`http://dbpedia.org/property/writer`, v3.`http://dbpedia.org/property/nuts`
