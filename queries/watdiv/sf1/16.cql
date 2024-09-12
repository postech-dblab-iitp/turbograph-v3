MATCH (v0)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v101),
(v0)-[:`http://db.uwaterloo.ca/~galuc/wsdbm/hasGenre`]->(v3)
WHERE v101.uri = "http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory10" AND 
v0.`http://schema.org/caption` IS NOT NULL AND 
v0.`http://schema.org/publisher` IS NOT NULL
RETURN v0, v0.`http://schema.org/caption`, v3, v0.`http://schema.org/publisher`