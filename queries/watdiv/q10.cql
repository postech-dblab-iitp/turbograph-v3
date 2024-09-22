MATCH (v0)-[:`http://schema.org/language`]->(v1),
(v2)-[:`http://schema.org/language`]->(v1),
(v4)-[:`http://db.uwaterloo.ca/~galuc/wsdbm/subscribes`]->(v2)
WHERE v2.`http://schema.org/url` IS NOT NULL
RETURN v0, v1, v2, v4