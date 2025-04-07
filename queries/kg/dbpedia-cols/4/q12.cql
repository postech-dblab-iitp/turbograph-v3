MATCH (v1)-[:`http://dbpedia.org/ontology/thumbnail`]->(v2), 
(v1)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v3), 
(v1)-[:`http://xmlns.com/foaf/0.1/homepage`]->(v5)
WHERE v1.`http://www.w3.org/2000/01/rdf-schema#label` IS NOT NULL
OPTIONAL MATCH (v1)-[:`http://xmlns.com/foaf/0.1/homepage`]->(v6)
RETURN v5.`http://www.w3.org/2003/01/geo/wgs84_pos#long`, v5.`http://www.w3.org/2000/01/rdf-schema#label`, v5.`http://www.w3.org/2003/01/geo/wgs84_pos#lat`, v5.`http://www.w3.org/2000/01/rdf-schema#comment`, v1.`http://dbpedia.org/ontology/areaTotal`, v1.`http://dbpedia.org/property/numberOfFilms`, v1.`http://dbpedia.org/ontology/podiums`, v1.`http://dbpedia.org/property/caps`, v2.`http://www.w3.org/2003/01/geo/wgs84_pos#lat`, v2.`http://dbpedia.org/ontology/wikiPageID`, v2.`http://www.w3.org/2003/01/geo/wgs84_pos#long`, v2.`http://dbpedia.org/ontology/abstract`

