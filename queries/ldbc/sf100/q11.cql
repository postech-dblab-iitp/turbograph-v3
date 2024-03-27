MATCH (person:Person {id: 2199023472955 })-[:KNOWS]->(friend:Person),
(friend)<-[:POST_HAS_CREATOR]-(post:Post)-[:POST_HAS_TAG]->(tag:Tag)
WITH DISTINCT tag, post
WITH tag,
    CASE
    WHEN post.creationDate >= 1317427200000 AND post.creationDate < 1321920000000 THEN 1
    ELSE 0
    END AS valid,
    CASE
    WHEN post.creationDate < 1317427200000 THEN 1
    ELSE 0
    END AS inValid
WITH tag.id AS tagid, tag.name AS tagName, sum(valid) AS postCount, sum(inValid) AS inValidPostCount
WHERE postCount > 0 AND inValidPostCount = 0
RETURN tagName, postCount
ORDER BY postCount DESC, tagName ASC
LIMIT 10;