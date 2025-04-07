MATCH (v1)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v2), 
(v1)-[:`http://dbpedia.org/ontology/foundationPlace`]->(v3), 
(v4)-[:`http://dbpedia.org/ontology/developer`]->(v1), 
(v4)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v5)
RETURN v1.`http://dbpedia.org/ontology/foundingYear`, v1.`http://dbpedia.org/ontology/production`, v1.`http://dbpedia.org/ontology/equity`, v4.`http://dbpedia.org/property/published`, v4.`http://dbpedia.org/property/voices`, v4.`http://dbpedia.org/ontology/numberOfRooms`, v3.`http://dbpedia.org/property/ethnicGroupsYear`, v3.`http://dbpedia.org/property/nickname`, v3.`http://dbpedia.org/property/managementElevation`