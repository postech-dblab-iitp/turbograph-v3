MATCH (n0)-[:`TEN_PRCNT`]->(n1)
RETURN 
COUNT(n0.`http://dbpedia.org/property/closed`), 
COUNT(n0.`http://dbpedia.org/property/debut`), 
COUNT(n0.`http://dbpedia.org/property/opentheme`), 
COUNT(n0.`http://dbpedia.org/ontology/alias`), 
COUNT(n0.`http://dbpedia.org/property/knownFor`), 
COUNT(n1.`http://dbpedia.org/property/iupacname`), 
COUNT(n1.`http://dbpedia.org/property/share`), 
COUNT(n1.`http://dbpedia.org/ontology/projectBudgetFunding`), 
COUNT(n1.`http://dbpedia.org/property/managementBody`), 
COUNT(n1.`http://dbpedia.org/property/mascot`);
