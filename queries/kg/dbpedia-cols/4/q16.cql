MATCH (v1)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v2)
WHERE v1.`http://dbpedia.org/property/population` IS NOT NULL OR
v1.`http://dbpedia.org/property/populationUrban` IS NOT NULL
RETURN v1.uri, v2.uri, v1.`http://dbpedia.org/ontology/utcOffset`, v1.`http://dbpedia.org/ontology/MeanOfTransportation/width`, v1.`http://dbpedia.org/property/yearStart`, v1.`http://dbpedia.org/ontology/budget`