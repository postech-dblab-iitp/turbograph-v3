MATCH (v0)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v1),
(v101)-[:`http://db.uwaterloo.ca/~galuc/wsdbm/likes`]->(v0)
WHERE v0.`http://schema.org/text` IS NOT NULL AND 
v101.uri = "http://db.uwaterloo.ca/~galuc/wsdbm/User3907"
RETURN v0, v1, v0.`http://schema.org/text`