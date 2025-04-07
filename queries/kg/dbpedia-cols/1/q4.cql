MATCH (v1)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v2), 
(v1)-[:`http://dbpedia.org/ontology/foundationPlace`]->(v3), 
(v4)-[:`http://dbpedia.org/ontology/developer`]->(v1), 
(v4)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v5)
RETURN v1.`http://dbpedia.org/property/airdate`, v4.`http://dbpedia.org/property/released`, v3.`http://dbpedia.org/ontology/governmentElevation`