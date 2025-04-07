MATCH (v1)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v2), 
(v1)-[:`http://xmlns.com/foaf/0.1/homepage`]->(v4)
WHERE v1.`http://dbpedia.org/property/numEmployees` IS NOT NULL
RETURN v4.`http://www.w3.org/2000/01/rdf-schema#comment`, v4.`http://www.w3.org/2000/01/rdf-schema#label`, v1.`http://dbpedia.org/ontology/foundingYear`, v1.`http://dbpedia.org/property/clubname`