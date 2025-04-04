MATCH 
(v6)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v4)
WHERE
v4.uri = "http://dbpedia.org/ontology/Place" AND
v6.`http://www.w3.org/2003/01/geo/wgs84_pos#lat` IS NOT NULL AND
v6.`http://www.w3.org/2003/01/geo/wgs84_pos#long` IS NOT NULL AND
v6.`http://www.w3.org/2000/01/rdf-schema#label` IS NOT NULL AND
v6.`http://dbpedia.org/ontology/abstract` IS NOT NULL
WITH v6
OPTIONAL MATCH (v6)-[:`http://xmlns.com/foaf/0.1/depiction`]->(v8)
OPTIONAL MATCH (v6)-[:`http://xmlns.com/foaf/0.1/homepage`]->(v10)
OPTIONAL MATCH (v6)-[:`http://dbpedia.org/ontology/thumbnail`]->(v14)
RETURN v6.`http://www.w3.org/2000/01/rdf-schema#label` AS r1, v6.`http://www.w3.org/2003/01/geo/wgs84_pos#long`, v6.`http://dbpedia.org/ontology/cost`, v6.`http://dbpedia.org/property/iata`, v6.`http://dbpedia.org/property/type`, v6.`http://dbpedia.org/ontology/geologicPeriod`, v10.`http://www.w3.org/2000/01/rdf-schema#comment`, v10.`http://www.w3.org/2000/01/rdf-schema#label`, v10.`http://dbpedia.org/ontology/wikiPageRevisionID`, v10.`http://www.georss.org/georss/point`, v10.`http://dbpedia.org/ontology/abstract`, v8.`http://www.w3.org/2003/01/geo/wgs84_pos#long`, v8.`http://www.w3.org/2000/01/rdf-schema#label`, v8.`http://dbpedia.org/ontology/conservationStatusSystem`, v8.`http://www.georss.org/georss/point`, v8.`http://dbpedia.org/property/status`, v14.`http://dbpedia.org/ontology/abstract`, v14.`http://www.w3.org/2000/01/rdf-schema#label`, v14.`http://www.w3.org/2003/01/geo/wgs84_pos#lat`, v14.`http://www.w3.org/2003/01/geo/wgs84_pos#long`, v14.`http://dbpedia.org/ontology/wikiPageRevisionID`