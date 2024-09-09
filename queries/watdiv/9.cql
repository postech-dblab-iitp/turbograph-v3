MATCH (v0)-[:`http://db.uwaterloo.ca/~galuc/wsdbm/subscribes`]->(v101),
(v0)-[:`http://db.uwaterloo.ca/~galuc/wsdbm/likes`]->(v2)
WHERE v101.uri = "http://db.uwaterloo.ca/~galuc/wsdbm/Website36" AND 
v2.`http://schema.org/caption` IS NOT NULL
RETURN v0.id, v2.id, v2.`http://schema.org/caption`