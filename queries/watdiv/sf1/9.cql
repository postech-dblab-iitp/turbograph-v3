MATCH (v0)-[:`http://db.uwaterloo.ca/~galuc/wsdbm/subscribes`]->(v101),
(v0)-[:`http://db.uwaterloo.ca/~galuc/wsdbm/likes`]->(v2)
WHERE v101.uri = "http://db.uwaterloo.ca/~galuc/wsdbm/Website25" AND 
v2.`http://schema.org/caption` IS NOT NULL
RETURN v0, v2, v2.`http://schema.org/caption`