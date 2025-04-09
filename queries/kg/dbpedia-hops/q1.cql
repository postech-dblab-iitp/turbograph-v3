MATCH (n0)-[:`TEN_PRCNT`]->(n1) 
RETURN 
COUNT(n0.`http://dbpedia.org/property/lowusage`), 
COUNT(n0.`http://dbpedia.org/property/stations`), 
COUNT(n0.`http://dbpedia.org/property/r6Surface`), 
COUNT(n1.`http://dbpedia.org/ontology/impactFactorAsOf`), 
COUNT(n1.`http://dbpedia.org/property/elevationMaxM`), 
COUNT(n1.`http://dbpedia.org/property/ranking`);
