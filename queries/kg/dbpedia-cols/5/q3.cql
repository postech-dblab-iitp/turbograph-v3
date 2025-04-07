MATCH (v1)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v2), 
(v1)-[:`http://dbpedia.org/property/website`]->(v4)
WHERE v1.`http://www.w3.org/2000/01/rdf-schema#label` IS NOT NULL
RETURN v4.`http://www.w3.org/2000/01/rdf-schema#label`, v4.`http://dbpedia.org/ontology/foundingYear`, v4.`http://www.georss.org/georss/point`, v4.`http://www.w3.org/2003/01/geo/wgs84_pos#long`, v4.`http://xmlns.com/foaf/0.1/name`, v1.`http://dbpedia.org/property/years`, v1.`http://dbpedia.org/property/haircolour`, v1.`http://dbpedia.org/property/founded`, v1.`http://dbpedia.org/property/period`, v1.`http://dbpedia.org/property/weightLb`
