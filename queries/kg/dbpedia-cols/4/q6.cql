MATCH (v1)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v2)
RETURN DISTINCT v1.uri AS r1, v1.`http://dbpedia.org/ontology/mass`, v1.`http://dbpedia.org/property/no`, v1.`http://dbpedia.org/ontology/averageDepth`, v1.`http://dbpedia.org/property/regnum`
