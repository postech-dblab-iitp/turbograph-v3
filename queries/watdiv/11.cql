MATCH (v0)-[:`http://db.uwaterloo.ca/~galuc/wsdbm/likes`]->(v1),
(v0)-[:`http://db.uwaterloo.ca/~galuc/wsdbm/subscribes`]->(v101)
WHERE v101.uri = "http://db.uwaterloo.ca/~galuc/wsdbm/Retailer3"
RETURN v0.id, v1.id