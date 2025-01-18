MATCH (v1)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v2), 
(v1)-[:`http://dbpedia.org/property/website`]->(v4)
WHERE v2.uri = "http://dbpedia.org/ontology/Person" AND v1.`http://www.w3.org/2000/01/rdf-schema#label` IS NOT NULL
RETURN v1.`http://www.w3.org/2000/01/rdf-schema#label` AS r1, v1.`http://purl.org/dc/elements/1.1/type`, v1.`http://dbpedia.org/property/status`, v1.`http://dbpedia.org/ontology/isoCodeRegion`, v1.`http://dbpedia.org/property/shipName`, v1.`http://dbpedia.org/property/commands`, v4.`http://dbpedia.org/property/name`, v4.`http://dbpedia.org/ontology/wikiPageID`, v4.`http://dbpedia.org/property/type`, v4.`http://www.georss.org/georss/point`, v4.`http://dbpedia.org/ontology/numberOfEmployees`
