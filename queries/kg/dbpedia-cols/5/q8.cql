MATCH (v1)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v2), 
(v1)-[:`http://dbpedia.org/ontology/nationality`]->(v3)
WHERE v3.`http://www.w3.org/2000/01/rdf-schema#label` IS NOT NULL AND 
v1.`http://www.w3.org/2000/01/rdf-schema#label` IS NOT NULL
RETURN v3.`http://dbpedia.org/ontology/yearOfConstruction`, v3.`http://dbpedia.org/property/type`, v3.`http://dbpedia.org/property/name`, v3.`http://dbpedia.org/property/populationRank`, v3.`http://dbpedia.org/property/filename`, v1.`http://dbpedia.org/ontology/birthDate`, v1.`http://dbpedia.org/ontology/title`, v1.`http://dbpedia.org/property/hairColour`, v1.`http://dbpedia.org/property/period`, v1.`http://dbpedia.org/property/otherNames`