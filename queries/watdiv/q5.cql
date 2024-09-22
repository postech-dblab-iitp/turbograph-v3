MATCH (v0)-[:`http://purl.org/goodrelations/includes`]->(v1),
(v2)-[:`http://purl.org/goodrelations/offers`]->(v0),
(v0)-[:`http://schema.org/eligibleRegion`]->(v101)
RETURN v0, v1, v2