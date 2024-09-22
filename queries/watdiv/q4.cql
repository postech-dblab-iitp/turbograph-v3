MATCH (v0)-[:`http://purl.org/goodrelations/includes`]->(v1),
(v1)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v2),
(v1)-[:`http://db.uwaterloo.ca/~galuc/wsdbm/hasGenre`]->(v5),
(v6)-[:`http://db.uwaterloo.ca/~galuc/wsdbm/likes`]->(v1)
WHERE v1.`http://schema.org/description` IS NOT NULL AND 
v1.`http://schema.org/text` IS NOT NULL 
RETURN v0, v1, v2, v5, v6