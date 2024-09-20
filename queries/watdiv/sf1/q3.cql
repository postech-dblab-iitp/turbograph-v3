MATCH (v0)-[:`http://ogp.me/ns#tag`]->(v2),
(v4)-[:`http://db.uwaterloo.ca/~galuc/wsdbm/purchaseFor`]->(v0)
WHERE v0.`http://schema.org/description` IS NOT NULL AND 
v0.`http://schema.org/text` IS NOT NULL
RETURN v0, v4