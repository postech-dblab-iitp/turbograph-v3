MATCH (v1)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v2)
RETURN v2.`http://www.w3.org/2003/01/geo/wgs84_pos#lat`, v2.`http://www.w3.org/2003/01/geo/wgs84_pos#long`, v1.`http://dbpedia.org/property/following`, v1.`http://dbpedia.org/property/areaHa`
