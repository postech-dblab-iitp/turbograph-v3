MATCH (v0)-[:`http://www.geonames.org/ontology#parentCountry`]->(v1),
(v2)-[:`http://purl.org/goodrelations/includes`]->(v3),
(v4)-[:`http://purl.org/goodrelations/offers`]->(v2),
(v2)-[:`http://schema.org/eligibleRegion`]->(v1),
(v6)-[:`http://db.uwaterloo.ca/~galuc/wsdbm/likes`]->(v3)
WHERE v2.`http://purl.org/goodrelations/price` IS NOT NULL
RETURN v0, v1, v2, v3, v4, v6