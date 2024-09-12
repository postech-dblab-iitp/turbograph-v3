MATCH (v0)-[:`http://ogp.me/ns#tag`]->(v101)
WHERE v101.uri = "http://db.uwaterloo.ca/~galuc/wsdbm/Topic127" AND 
v0.`http://schema.org/caption` IS NOT NULL
RETURN v0, v0.`http://schema.org/caption`