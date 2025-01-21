MATCH (v1)-[:`http://purl.org/dc/terms/subject`]->(v2)
WHERE v1.`http://www.w3.org/2000/01/rdf-schema#label` IS NOT NULL
RETURN DISTINCT v2.uri