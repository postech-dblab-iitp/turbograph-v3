MATCH (v0)-[:`http://purl.org/goodrelations/includes`]->(v1),
(v2)-[:`http://purl.org/goodrelations/offers`]->(v0),
(v1)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v4),
(v0)-[:`http://schema.org/eligibleRegion`]->(v7),
(v8)-[:`http://db.uwaterloo.ca/~galuc/wsdbm/purchaseFor`]->(v1)
WHERE v0.`http://purl.org/goodrelations/validFrom` IS NOT NULL AND 
v1.`http://schema.org/contentRating` IS NOT NULL
RETURN v0, v1, v2, v4, v7, v8