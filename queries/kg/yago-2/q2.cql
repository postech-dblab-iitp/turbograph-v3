MATCH (v0)-[:`http://schema.org/location`]->(v1), 
(v1)-[:`http://www.w3.org/2002/07/owl#sameAs`]->(v2) 
WHERE v0.`http://www.w3.org/2000/01/rdf-schema#comment` IS NOT NULL 
AND v1.`http://schema.org/postalCode` IS NOT NULL 
RETURN v0.`http://www.w3.org/2000/01/rdf-schema#comment` AS `v0_rdf-schema#comment`,
v1.`http://yago-knowledge.org/resource/demonym` AS `v1_demonym` 
ORDER BY `v0_rdf-schema#comment`, `v1_demonym`