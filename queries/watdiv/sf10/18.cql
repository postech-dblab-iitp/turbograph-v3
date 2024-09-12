MATCH (v0)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v101),
(v0)-[:`http://schema.org/language`]->(v102)
WHERE v101.uri = "http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory6" AND 
v0.`http://schema.org/description` IS NOT NULL AND 
v0.`http://schema.org/keywords` IS NOT NULL AND 
v102.uri = "http://db.uwaterloo.ca/~galuc/wsdbm/Language0"
RETURN v0, v0.`http://schema.org/description`, v0.`http://schema.org/keywords`