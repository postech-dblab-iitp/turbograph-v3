MATCH (person:Person {id: 21990232559429 })-[:KNOWS]->(friend:Person),
(friend)<-[:POST_HAS_CREATOR]-(post:Post)-[:POST_HAS_TAG]->(tag:Tag)
WITH DISTINCT tag, post
WITH tag,
    CASE
    WHEN post.creationDate >= 1335830400000 AND post.creationDate < 1339027200000 THEN 1
    ELSE 0
    END AS valid,
    CASE
    WHEN post.creationDate < 1335830400000 THEN 1
    ELSE 0
    END AS inValid
WITH tag.id AS tagid, tag.name AS tagName, sum(valid) AS postCount, sum(inValid) AS inValidPostCount
WHERE postCount > 0 AND inValidPostCount = 0
RETURN tagName, postCount
ORDER BY postCount DESC, tagName ASC
LIMIT 10;