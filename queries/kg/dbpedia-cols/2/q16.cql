MATCH (v1)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v2)
WHERE v1.`http://dbpedia.org/property/population` IS NOT NULL OR
v1.`http://dbpedia.org/property/populationUrban` IS NOT NULL
RETURN v1.`http://dbpedia.org/ontology/areaUrban`, v1.`http://www.w3.org/2003/01/geo/wgs84_pos#lat`