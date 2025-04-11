MATCH (n0)-[:`BI_TEN_PRCNT`]->(n1)
RETURN
COUNT(n1.`http://dbpedia.org/property/iupacname`), 
COUNT(n1.`http://dbpedia.org/property/share`), 
COUNT(n1.`http://dbpedia.org/ontology/projectBudgetFunding`), 
COUNT(n1.`http://dbpedia.org/property/managementBody`), 
COUNT(n1.`http://dbpedia.org/property/mascot`);
