MATCH (v1)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v2), 
(v1)-[:`http://dbpedia.org/ontology/nationality`]->(v3)
WHERE 
v3.`http://www.w3.org/2000/01/rdf-schema#label` IS NOT NULL AND 
v1.`http://www.w3.org/2000/01/rdf-schema#label` IS NOT NULL
RETURN v3.`http://dbpedia.org/property/timezone1Dst`, v3.`http://dbpedia.org/property/bronze`, v3.`http://dbpedia.org/property/areaUrbanKm`, v1.`http://dbpedia.org/property/club`, v1.`http://dbpedia.org/property/order`, v1.`http://dbpedia.org/property/department`