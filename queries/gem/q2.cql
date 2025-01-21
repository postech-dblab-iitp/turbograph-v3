MATCH
(concept)<-[:`http://purl.org/dc/terms/subject`]-(industry),
(industry)<-[:`http://dbpedia.org/ontology/industry`]-(company)-[:`http://dbpedia.org/ontology/locationCity`]->(city)<-[:`http://dbpedia.org/ontology/locationCity`]-(subcompany)-[:`http://dbpedia.org/ontology/industry`]->(subIndustry)-[:`http://purl.org/dc/terms/subject`]->(subConcept)
WHERE 
  concept.uri = "http://dbpedia.org/resource/Category:Digital_technology" 
RETURN 
  city.`http://www.w3.org/2000/01/rdf-schema#label` AS city_name,
  subIndustry.`http://www.w3.org/2000/01/rdf-schema#label` AS industry_name,
  subConcept.`http://www.w3.org/2000/01/rdf-schema#label` AS concept_name
ORDER BY 
  city_name DESC
