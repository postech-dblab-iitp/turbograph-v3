MATCH (v0)-[:`http://purl.org/dc/terms/Location`]->(v1),
(v0)-[:`http://schema.org/nationality`]->(v101),
(v0)-[:`http://db.uwaterloo.ca/~galuc/wsdbm/gender`]->(v3),
(v0)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v102)
WHERE v101.uri = "http://db.uwaterloo.ca/~galuc/wsdbm/Country16" AND 
v102.uri = "http://db.uwaterloo.ca/~galuc/wsdbm/Role2"
RETURN v0, v1, v3