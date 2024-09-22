MATCH (v0)-[:`http://ogp.me/ns#tag`]->(v1),
(v101)-[:`http://ogp.me/ns#tag`]->(v1)
WHERE v101.uri = "http://db.uwaterloo.ca/~galuc/wsdbm/SubGenre92"
RETURN v0, v1