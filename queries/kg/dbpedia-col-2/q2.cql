MATCH (v1)-[:`http://xmlns.com/foaf/0.1/homepage`]->(v2), 
(v1)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v3)
RETURN v3.uri AS r1, v2.`http://dbpedia.org/ontology/wikiPageID`, v2.`http://www.w3.org/2003/01/geo/wgs84_pos#long`, v1.`http://dbpedia.org/property/closed`, v1.`http://dbpedia.org/ontology/ranking`