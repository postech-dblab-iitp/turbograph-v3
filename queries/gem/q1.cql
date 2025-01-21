MATCH
(country)<-[:`http://dbpedia.org/ontology/country`]-(birthPlace)<-[:`http://dbpedia.org/ontology/birthPlace`]-(person)<-[:`http://dbpedia.org/ontology/keyPerson`]-(company)-[:`http://dbpedia.org/ontology/locationCity`]->(city),
(company)-[:`http://dbpedia.org/ontology/industry`]->(industry)-[:`http://purl.org/dc/terms/subject`]->(concept)

WHERE
country.uri = "http://dbpedia.org/resource/United_States" AND
 concept.uri = "http://dbpedia.org/resource/Category:Digital_technology" 

RETURN 
person.uri, birthPlace.uri, city.uri