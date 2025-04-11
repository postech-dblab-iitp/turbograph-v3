MATCH (n0)-[:`BI_FIVE_TEN_PRCNT`]->(n1)-[:`DI_POINT_FIVE_PRCNT`]->(n2) 
RETURN 
COUNT(n1.`http://dbpedia.org/property/ceeb`), 
COUNT(n1.`http://dbpedia.org/property/twin7Country`), 
COUNT(n1.`http://dbpedia.org/property/venue`), 
COUNT(n1.`http://dbpedia.org/property/mainSettlement`), 
COUNT(n1.`http://dbpedia.org/property/minister1Name`), 
COUNT(n2.`http://dbpedia.org/property/goldenbauhiniaawards`), 
COUNT(n2.`http://dbpedia.org/property/inaugurationDate`), 
COUNT(n2.`http://dbpedia.org/property/voicedBy`), 
COUNT(n2.`http://dbpedia.org/property/imageBathymetry`), 
COUNT(n2.`http://dbpedia.org/property/aka`);
