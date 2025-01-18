MATCH (v1)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v2)
RETURN DISTINCT v1.uri AS r1, v2.`http://www.georss.org/georss/point`, v2.`http://www.w3.org/2003/01/geo/wgs84_pos#lat`, v1.`http://dbpedia.org/property/postalCode`, v1.`http://dbpedia.org/property/ended`
