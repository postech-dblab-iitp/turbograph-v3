MATCH (v0)-[:`http://purl.org/goodrelations/offers`]->(v2),
(v2)-[:`http://schema.org/eligibleRegion`]->(v101),
(v2)-[:`http://purl.org/goodrelations/includes`]->(v3),
(v4)-[:`http://xmlns.com/foaf/homepage`]->(v6),
(v4)-[:`http://db.uwaterloo.ca/~galuc/wsdbm/makesPurchase`]->(v7),
(v7)-[:`http://db.uwaterloo.ca/~galuc/wsdbm/purchaseFor`]->(v3),
(v3)-[:`http://purl.org/stuff/rev#hasReview`]->(v8)
WHERE v0.`http://schema.org/legalName` IS NOT NULL AND 
v101.uri = "http://db.uwaterloo.ca/~galuc/wsdbm/Country5" AND 
v4.`http://schema.org/jobTitle` IS NOT NULL AND 
v8.`http://purl.org/stuff/rev#totalVotes` IS NOT NULL
RETURN v0.id, v3.id, v4.id, v8.id