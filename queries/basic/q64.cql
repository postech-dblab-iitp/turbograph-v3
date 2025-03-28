MATCH (n) WHERE n.`http://dbpedia.org/ontology/temperature` > 28 AND n.`http://dbpedia.org/ontology/temperature` < 305 RETURN n.`http://dbpedia.org/ontology/temperature`;
