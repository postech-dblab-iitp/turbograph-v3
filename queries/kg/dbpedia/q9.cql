MATCH (v1)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v2)
WHERE v1.`http://www.w3.org/2000/01/rdf-schema#label` IS NOT NULL
AND REGEXP_MATCHES(v1.`http://www.w3.org/2000/01/rdf-schema#label`, '.*pes.*')
RETURN DISTINCT v2.uri AS r1