MATCH (person:Person { id: 8796093163356 })-[:KNOWS*1..2]-(friend:Person)
WHERE NOT person = friend
WITH DISTINCT friend
MATCH (friend)<-[membership:HAS_MEMBER]-(forum:Forum)
WHERE
    membership.joinDate > 1348272000000
WITH
    forum,
    friend
MATCH (friend)<-[:POST_HAS_CREATOR]-(post:Post)<-[:CONTAINER_OF]-(forum)
WITH
    forum,
    count(post) AS postCount
RETURN
    forum.title AS forumName,
    postCount,
    forum.id AS forumId
ORDER BY
    postCount DESC,
    forumId ASC
LIMIT 20;