MATCH (v0)-[:`http://xmlns.com/foaf/homepage`]->(v1),
(v2)-[:`http://purl.org/goodrelations/includes`]->(v0),
(v0)-[:`http://ogp.me/ns#tag`]->(v101),
(v1)-[:`http://schema.org/language`]->(v102),
(v7)-[:`http://db.uwaterloo.ca/~galuc/wsdbm/likes`]->(v0)
WHERE v101.uri = "http://db.uwaterloo.ca/~galuc/wsdbm/SubGenre103" AND 
v0.`http://schema.org/description` IS NOT NULL AND 
v0.`http://schema.org/contentSize` IS NOT NULL AND 
v1.`http://schema.org/url` IS NOT NULL AND 
v1.`http://db.uwaterloo.ca/~galuc/wsdbm/hits` IS NOT NULL AND 
v102.uri = "http://db.uwaterloo.ca/~galuc/wsdbm/Language0"
RETURN v0.id, v1.id, v2.id, v0.`http://schema.org/description`, v1.`http://schema.org/url`, v1.`http://db.uwaterloo.ca/~galuc/wsdbm/hits`, v7.id, v0.`http://schema.org/contentSize`