MATCH (v1)-[:`http://xmlns.com/foaf/0.1/homepage`]->(v2), 
(v1)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v3)
RETURN v2.`http://dbpedia.org/ontology/wikiPageRevisionID`, v2.`http://dbpedia.org/ontology/abstract`, v1.`http://dbpedia.org/property/nextAlbum`, v1.`http://dbpedia.org/ontology/ingredientName`