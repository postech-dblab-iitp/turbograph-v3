MATCH (v1)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v2)
WHERE v1.`http://www.w3.org/2000/01/rdf-schema#label` IS NOT NULL
AND REGEXP_MATCHES(v1.`http://www.w3.org/2000/01/rdf-schema#label`, '.*pes.*')
RETURN v2.`http://www.w3.org/2003/01/geo/wgs84_pos#lat`, v2.`http://www.georss.org/georss/point`, v1.`http://dbpedia.org/ontology/firstAirDate`, v1.`http://dbpedia.org/property/lengthKm`