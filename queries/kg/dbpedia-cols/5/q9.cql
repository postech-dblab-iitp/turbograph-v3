MATCH (v1)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v2)
WHERE v1.`http://www.w3.org/2000/01/rdf-schema#label` IS NOT NULL
AND v1.`http://www.w3.org/2000/01/rdf-schema#label` =~ '(?i).*pes.*'
RETURN DISTINCT v2.uri AS r1, v1.`http://dbpedia.org/property/developer`, v1.`http://dbpedia.org/property/catches`, v1.`http://dbpedia.org/ontology/allegiance`, v1.`http://dbpedia.org/property/mouthLocation`, v1.`http://dbpedia.org/ontology/Infrastructure/length`