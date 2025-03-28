MATCH (n) WHERE n.`http://dbpedia.org/property/altitude` > 10 AND n.`http://dbpedia.org/property/altitude` < 100 RETURN n.`http://dbpedia.org/property/altitude`;
