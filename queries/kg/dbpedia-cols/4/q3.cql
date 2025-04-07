MATCH (v1)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v2), 
(v1)-[:`http://dbpedia.org/property/website`]->(v4)
WHERE v1.`http://www.w3.org/2000/01/rdf-schema#label` IS NOT NULL
RETURN v4.`http://dbpedia.org/property/name`, v4.`http://dbpedia.org/property/numEmployees`, v4.`http://www.w3.org/2003/01/geo/wgs84_pos#lat`, v4.`http://dbpedia.org/ontology/wikiPageID`, v1.`http://dbpedia.org/property/billed`, v1.`http://dbpedia.org/property/fastestLaps`, v1.`http://dbpedia.org/property/dateOfDeath`, v1.`http://dbpedia.org/ontology/Stream/discharge`
