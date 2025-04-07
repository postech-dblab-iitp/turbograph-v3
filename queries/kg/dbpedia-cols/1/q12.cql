MATCH (v1)-[:`http://dbpedia.org/ontology/thumbnail`]->(v2), 
(v1)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v3), 
(v1)-[:`http://xmlns.com/foaf/0.1/homepage`]->(v5)
WHERE v1.`http://www.w3.org/2000/01/rdf-schema#label` IS NOT NULL
OPTIONAL MATCH (v1)-[:`http://xmlns.com/foaf/0.1/homepage`]->(v6)
RETURN v5.`http://www.w3.org/2000/01/rdf-schema#comment`, v6.`http://www.w3.org/2003/01/geo/wgs84_pos#long`, v1.`http://dbpedia.org/ontology/militaryCommand`, v2.`http://dbpedia.org/ontology/abstract`

