MATCH (n0)-[:`ONE_PRCNT`]->(n1)<-[:`ONE_PRCNT`]-(n2) 
RETURN 
COUNT(n0.`http://dbpedia.org/property/lowusage`), 
COUNT(n0.`http://dbpedia.org/property/stations`),
COUNT(n0.`http://dbpedia.org/ontology/impactFactorAsOf`), 
COUNT(n1.`http://dbpedia.org/property/athletesParticipating`), 
COUNT(n1.`http://dbpedia.org/property/popularVote`), 
COUNT(n1.`http://dbpedia.org/ontology/compressionRatio`), 
COUNT(n2.`http://dbpedia.org/property/weight_lb__`), 
COUNT(n2.`http://dbpedia.org/property/shireCounty`), 
COUNT(n2.`http://dbpedia.org/property/ranking`);
