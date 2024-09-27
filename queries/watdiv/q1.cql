MATCH (v0)-[:`http://ogp.me/ns#tag`]->(v1),
(v2)-[:`http://ogp.me/ns#tag`]->(v1),
(v4)-[:`http://db.uwaterloo.ca/~galuc/wsdbm/likes`]->(v0)
WHERE v0.`http://schema.org/caption` IS NOT NULL
AND v2.`http://schema.org/text` IS NOT NULL
RETURN v0, v1, v2, v4