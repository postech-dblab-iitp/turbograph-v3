MATCH (root:Person {id: 2199023323088 })-[:KNOWS*1..2]->(friend:Person)
WHERE NOT friend = root
WITH DISTINCT friend
MATCH (friend)<-[:HAS_CREATOR]-(message:Comment)
WHERE message.creationDate < 1333670400000
RETURN
    friend.id AS personId,
    friend.firstName AS personFirstName,
    friend.lastName AS personLastName,
    message.id AS commentOrPostId,
    message.content AS commentOrPostContent,
    message.creationDate AS commentOrPostCreationDate
ORDER BY
    commentOrPostCreationDate DESC,
    commentOrPostId ASC
LIMIT 20