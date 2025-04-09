MATCH (n0)-[:`ONE_PRCNT`]->(n1)-[:`THREE_PRCNT`]->(n2)<-[:`ONE_PRCNT`]-(n3) 
RETURN 
COUNT(n0.`http://dbpedia.org/ontology/conservationStatusSystem`), 
COUNT(n0.`http://dbpedia.org/ontology/fareZone`), 
COUNT(n0.`http://dbpedia.org/property/othertitles`), 
COUNT(n1.`http://dbpedia.org/property/staff`), 
COUNT(n1.`http://dbpedia.org/property/ball`), 
COUNT(n1.`http://dbpedia.org/property/binomialAuthority`),
COUNT(n2.`http://dbpedia.org/property/leaderTitle`), 
COUNT(n2.`http://dbpedia.org/property/roadName`), 
COUNT(n2.`http://dbpedia.org/property/schoolboard`), 
COUNT(n3.`http://dbpedia.org/property/stateLegislature`), 
COUNT(n3.`http://dbpedia.org/property/leagues`), 
COUNT(n3.`http://dbpedia.org/property/animal`);
