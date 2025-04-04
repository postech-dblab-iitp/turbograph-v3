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
RETURN v6.`http://www.w3.org/2000/01/rdf-schema#label` AS r1, v6.`http://dbpedia.org/property/construction`, v6.`http://dbpedia.org/property/otherName`, v6.`http://dbpedia.org/property/utcOffset1Dst`, v10.`http://www.georss.org/georss/point`, v10.`http://www.w3.org/2000/01/rdf-schema#label`, v10.`http://dbpedia.org/ontology/wikiPageID`, v8.`http://www.georss.org/georss/point`, v8.`http://dbpedia.org/property/name`, v8.`http://dbpedia.org/property/statusSystem`, v14.`http://dbpedia.org/ontology/wikiPageRevisionID`, v14.`http://www.georss.org/georss/point`, v14.`http://www.w3.org/2000/01/rdf-schema#comment`