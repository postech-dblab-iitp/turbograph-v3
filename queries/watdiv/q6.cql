MATCH (v0)-[:`http://purl.org/dc/terms/Location`]->(v1),
(v1)-[:`http://www.geonames.org/ontology#parentCountry`]->(v2),
(v3)-[:`http://schema.org/eligibleRegion`]->(v2)
WHERE v3.`http://purl.org/goodrelations/validFrom` IS NOT NULL AND 
v3.`http://schema.org/priceValidUntil` IS NOT NULL
RETURN v0, v1, v2, v3