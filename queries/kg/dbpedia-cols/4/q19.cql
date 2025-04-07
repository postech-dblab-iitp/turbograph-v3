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
RETURN v6.`http://dbpedia.org/property/equity`, v6.`http://dbpedia.org/property/revenue`, v6.`http://dbpedia.org/property/foundation`, v6.`http://dbpedia.org/property/nativeName`, v1.`http://dbpedia.org/ontology/gross`, v1.`http://dbpedia.org/property/commonName`, v1.`http://dbpedia.org/property/populationDate`, v1.`http://dbpedia.org/ontology/populationMetro`