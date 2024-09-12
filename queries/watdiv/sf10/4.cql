MATCH (v0)-[:`http://ogp.me/ns#tag`]->(v101),
(v0)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v2),
(v3)-[:`http://schema.org/trailer`]->(v4),
(v3)-[:`http://db.uwaterloo.ca/~galuc/wsdbm/hasGenre`]->(v0),
(v3)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v102)
WHERE v101.uri = "http://db.uwaterloo.ca/~galuc/wsdbm/Topic118" AND 
v3.`http://schema.org/keywords` IS NOT NULL AND 
v102.uri = "http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory2"
RETURN v0, v2, v3, v4, v3.`http://schema.org/keywords`