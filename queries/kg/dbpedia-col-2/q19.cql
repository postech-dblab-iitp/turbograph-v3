MATCH (v1)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]-(v2),
(v6)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]-(v7),
(v6)-[:`http://dbpedia.org/ontology/city`]->(v1)
WHERE 
v2.uri = "http://dbpedia.org/ontology/Settlement" AND
v1.`http://www.w3.org/2000/01/rdf-schema#label` IS NOT NULL AND
v7.uri = "http://dbpedia.org/ontology/Airport" AND
(
  v6.`http://dbpedia.org/property/iata` IS NOT NULL OR
  v6.`http://dbpedia.org/ontology/iataLocationIdentifier` IS NOT NULL
)
OPTIONAL MATCH (v6)-[:`http://xmlns.com/foaf/0.1/homepage`]->(v6_home)
RETURN v1.`http://www.w3.org/2000/01/rdf-schema#label` AS r1, v1.`http://dbpedia.org/property/timezone`, v1.`http://dbpedia.org/property/utcOffsetDst`, v6.`http://dbpedia.org/property/nativenameA`, v6.`http://www.georss.org/georss/point`