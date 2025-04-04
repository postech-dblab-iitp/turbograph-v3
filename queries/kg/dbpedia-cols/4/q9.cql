MATCH (v1)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v2)
WHERE v1.`http://www.w3.org/2000/01/rdf-schema#label` IS NOT NULL
AND v1.`http://www.w3.org/2000/01/rdf-schema#label` =~ '(?i).*pes.*'
RETURN DISTINCT v2.uri AS r1, v1.`http://dbpedia.org/ontology/formationYear`, v1.`http://dbpedia.org/property/populationDensitySqMi`, v1.`http://dbpedia.org/ontology/icd9`, v1.`http://dbpedia.org/property/careerprizemoney`