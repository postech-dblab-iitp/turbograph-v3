MATCH (v0)-[:`http://xmlns.com/foaf/homepage`]->(v1),
(v0)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v3),
(v0)-[:`http://db.uwaterloo.ca/~galuc/wsdbm/hasGenre`]->(v101)
WHERE v0.`http://ogp.me/ns#title` IS NOT NULL AND 
v0.`http://schema.org/caption` IS NOT NULL AND 
v0.`http://schema.org/description` IS NOT NULL AND 
v1.`http://schema.org/url` IS NOT NULL AND 
v1.`http://db.uwaterloo.ca/~galuc/wsdbm/hits` IS NOT NULL AND 
v101.uri = "http://db.uwaterloo.ca/~galuc/wsdbm/SubGenre63"
RETURN v0, v1, v0.`http://ogp.me/ns#title`, v0.`http://schema.org/caption`, v0.`http://schema.org/description`, v1.`http://schema.org/url`, v1.`http://db.uwaterloo.ca/~galuc/wsdbm/hits`