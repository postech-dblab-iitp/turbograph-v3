MATCH (v0)-[:`http://www.geonames.org/ontology#parentCountry`]->(v1),
(v2)-[:`http://purl.org/ontology/mo/performed_in`]->(v0),
(v2)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v4),
(v5)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v6),
(v5)-[:`http://schema.org/nationality`]->(v1)
WHERE v2.`http://purl.org/ontology/mo/performer` IS NOT NULL
RETURN v0, v1, v2, v4, v5, v6