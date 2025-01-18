MATCH (v1)-[:`http://dbpedia.org/ontology/thumbnail`]->(v2), 
(v1)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v3), 
(v1)-[:`http://xmlns.com/foaf/0.1/homepage`]->(v5)
WHERE v1.`http://www.w3.org/2000/01/rdf-schema#label` IS NOT NULL AND
v3.uri = "http://dbpedia.org/ontology/Person"
OPTIONAL MATCH (v1)-[:`http://xmlns.com/foaf/0.1/homepage`]->(v6)
RETURN v1.`http://www.w3.org/2000/01/rdf-schema#label` AS r1, v5.`http://dbpedia.org/ontology/wikiPageID`, v5.`http://www.w3.org/2000/01/rdf-schema#comment`, v5.`http://www.georss.org/georss/point`, v5.`http://dbpedia.org/ontology/abstract`, v5.`http://www.w3.org/2000/01/rdf-schema#label`, v5.`http://dbpedia.org/ontology/wikiPageRevisionID`, v5.`http://www.w3.org/2003/01/geo/wgs84_pos#long`, v5.`http://www.w3.org/2003/01/geo/wgs84_pos#lat`, v2.`http://dbpedia.org/ontology/wikiPageID`, v2.`http://www.w3.org/2000/01/rdf-schema#comment`, v2.`http://www.georss.org/georss/point`, v2.`http://dbpedia.org/ontology/abstract`, v2.`http://www.w3.org/2000/01/rdf-schema#label`, v2.`http://dbpedia.org/ontology/wikiPageRevisionID`, v2.`http://www.w3.org/2003/01/geo/wgs84_pos#long`, v2.`http://www.w3.org/2003/01/geo/wgs84_pos#lat`, 
v1.`http://dbpedia.org/property/basinCountries`, v1.`http://dbpedia.org/property/pictureFormat`, v1.`http://dbpedia.org/property/pastteams`, v1.`http://dbpedia.org/property/weightLb`, v1.`http://dbpedia.org/property/womensworldchampion`, v1.`http://dbpedia.org/property/areaTotalKm`, v1.`http://dbpedia.org/property/professional`, v1.`http://dbpedia.org/ontology/timeInSpace`, v1.`http://dbpedia.org/ontology/plays`, v1.`http://dbpedia.org/property/district`

