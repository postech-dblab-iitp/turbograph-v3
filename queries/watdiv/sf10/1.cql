MATCH (v0)-[:`http://purl.org/stuff/rev#hasReview`]->(v4),
(v4)-[:`http://purl.org/stuff/rev#reviewer`]->(v6),
(v7)-[:`http://schema.org/actor`]->(v6),
(v7)-[:`http://schema.org/language`]->(v8)
WHERE v0.`http://schema.org/caption` IS NOT NULL AND 
v0.`http://schema.org/text` IS NOT NULL AND 
v0.`http://schema.org/contentRating` IS NOT NULL AND 
v4.`http://purl.org/stuff/rev#title` IS NOT NULL
RETURN v0.id, v4.id, v6.id, v7.id