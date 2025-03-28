MATCH (n) WHERE n.`http://dbpedia.org/ontology/distanceLaps` > 10 AND n.`http://dbpedia.org/ontology/distanceLaps` < 100 RETURN n.`http://dbpedia.org/ontology/distanceLaps`;
