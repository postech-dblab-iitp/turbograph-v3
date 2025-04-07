MATCH (v1)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v2), 
(v1)-[:`http://xmlns.com/foaf/0.1/homepage`]->(v4)
WHERE v1.`http://dbpedia.org/property/numEmployees` IS NOT NULL
RETURN v4.`http://dbpedia.org/ontology/wikiPageRevisionID`, v4.`http://www.w3.org/2000/01/rdf-schema#comment`, v4.`http://www.w3.org/2000/01/rdf-schema#label`, v4.`http://www.w3.org/2003/01/geo/wgs84_pos#lat`, v1.`http://dbpedia.org/property/launchDate`, v1.`http://dbpedia.org/property/county`, v1.`http://dbpedia.org/ontology/utcOffset`, v1.`http://dbpedia.org/property/products`