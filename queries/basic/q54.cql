MATCH (n) WHERE n.`http://dbpedia.org/ontology/wins` > 10 AND n.`http://dbpedia.org/ontology/wins` < 100 RETURN n.`http://dbpedia.org/ontology/wins`;
