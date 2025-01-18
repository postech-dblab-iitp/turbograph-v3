MATCH (v1)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v2)
WHERE v1.`http://dbpedia.org/property/population` IS NOT NULL OR
v1.`http://dbpedia.org/property/populationUrban` IS NOT NULL
RETURN v1.uri, v2.uri, v1.`http://dbpedia.org/property/areaMetroKm`, v1.`http://dbpedia.org/property/gross`, v1.`http://dbpedia.org/ontology/prominence`, v1.`http://dbpedia.org/ontology/mouthElevation`, v1.`http://dbpedia.org/property/beds`