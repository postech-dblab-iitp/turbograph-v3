MATCH (n0)-[:`BI_THREE_PRCNT`]->(n1)-[:`BI_THREE_PRCNT`]->(n2)-[:`DI_POINT_ZERO_ONE_PRCNT`]->(n3) 
RETURN 
COUNT(n1.`http://dbpedia.org/property/weightLb`), 
COUNT(n1.`http://dbpedia.org/property/lowestelev`), 
COUNT(n1.`http://dbpedia.org/property/elevationMinM`), 
COUNT(n1.`http://dbpedia.org/property/populationDensityUrbanMi`), 
COUNT(n1.`http://dbpedia.org/ontology/unitedStatesNationalBridgeId`), 
COUNT(n2.`http://dbpedia.org/ontology/production`), 
COUNT(n2.`http://dbpedia.org/ontology/drugbank`), 
COUNT(n2.`http://dbpedia.org/property/highestLocation`), 
COUNT(n2.`http://dbpedia.org/ontology/projectReferenceID`), 
COUNT(n2.`http://dbpedia.org/property/interest`), 
COUNT(n3.`http://dbpedia.org/ontology/firstAscentYear`), 
COUNT(n3.`http://dbpedia.org/property/tribus`), 
COUNT(n3.`http://dbpedia.org/ontology/iso6393Code`), 
COUNT(n3.`http://dbpedia.org/ontology/openAccessContent`), 
COUNT(n3.`http://dbpedia.org/ontology/Person/height`);