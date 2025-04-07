MATCH (v1)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]-(v2),
(v6)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]-(v7),
(v6)-[:`http://dbpedia.org/ontology/city`]->(v1)
WHERE 
v1.`http://www.w3.org/2000/01/rdf-schema#label` IS NOT NULL AND
(
  v6.`http://dbpedia.org/property/iata` IS NOT NULL OR
  v6.`http://dbpedia.org/ontology/iataLocationIdentifier` IS NOT NULL
)
OPTIONAL MATCH (v6)-[:`http://xmlns.com/foaf/0.1/homepage`]->(v6_home)
RETURN  v6.`http://dbpedia.org/ontology/runwaySurface`, v6.`http://dbpedia.org/property/industry`, v6.`http://xmlns.com/foaf/0.1/name`, v6.`http://dbpedia.org/property/ownerOper`, v6.`http://dbpedia.org/property/utcOffsetDst`, v1.`http://dbpedia.org/property/mayor`, v1.`http://dbpedia.org/property/birthName`, v1.`http://dbpedia.org/ontology/minimumElevation`, v1.`http://dbpedia.org/property/part`, v1.`http://dbpedia.org/property/popDate`