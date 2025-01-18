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
RETURN v1.`http://www.w3.org/2000/01/rdf-schema#label` AS r1, v6.`http://dbpedia.org/property/r7Surface`, v6.`http://dbpedia.org/property/keyPeople`, v6.`http://dbpedia.org/property/utcOffsetDst`, v6.`http://dbpedia.org/property/r7LengthM`, v6.`http://dbpedia.org/property/r5LengthF`, v6.`http://dbpedia.org/ontology/wikiPageRevisionID`, v6.`http://www.w3.org/2003/01/geo/wgs84_pos#long`, v6.`http://dbpedia.org/property/assets`, v6.`http://dbpedia.org/property/built`, v6.`http://dbpedia.org/property/r6Surface`, v1.`http://dbpedia.org/property/leaderTitle`, v1.`http://dbpedia.org/property/divisions`, v1.`http://dbpedia.org/property/postal2Code`, v1.`http://dbpedia.org/ontology/utcOffset`, v1.`http://dbpedia.org/property/location`, v1.`http://dbpedia.org/ontology/areaMetro`, v1.`http://dbpedia.org/property/mayor`, v1.`http://dbpedia.org/property/plz`, v1.`http://dbpedia.org/property/pop`, v1.`http://dbpedia.org/property/gaelicName`