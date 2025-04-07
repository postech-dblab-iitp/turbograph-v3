MATCH (v1)-[:`http://dbpedia.org/ontology/thumbnail`]->(v2), 
(v1)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v3), 
(v1)-[:`http://xmlns.com/foaf/0.1/homepage`]->(v5)
WHERE v1.`http://www.w3.org/2000/01/rdf-schema#label` IS NOT NULL
OPTIONAL MATCH (v1)-[:`http://xmlns.com/foaf/0.1/homepage`]->(v6)
RETURN v5.`http://www.w3.org/2003/01/geo/wgs84_pos#lat`, v5.`http://www.w3.org/2003/01/geo/wgs84_pos#long`, v5.`http://dbpedia.org/ontology/abstract`, v5.`http://www.w3.org/2000/01/rdf-schema#comment`, v5.`http://www.w3.org/2000/01/rdf-schema#label`, v1.`http://dbpedia.org/property/alternativeNames`, v1.`http://dbpedia.org/ontology/elevation`, v1.`http://dbpedia.org/property/manageryears`, v1.`http://dbpedia.org/property/college`, v1.`http://dbpedia.org/property/first`, v2.`http://www.w3.org/2003/01/geo/wgs84_pos#long`, v2.`http://www.w3.org/2000/01/rdf-schema#label`, v2.`http://www.georss.org/georss/point`, v2.`http://dbpedia.org/ontology/abstract`, v2.`http://dbpedia.org/ontology/wikiPageRevisionID`

