MATCH (tag:Tag)-[:HAS_TYPE*0..5]->(baseTagClass:TagClass)
WHERE tag.name = 'Wrestler' or baseTagClass.name = 'Wrestler'
WITH tag
MATCH (person:Person {id: 2199023521857 })<-[:KNOWS]-(friend:Person)<-[:HAS_CREATOR]-(comment:Comment)-[:REPLY_OF]->(post:Post)-[:POST_HAS_TAG]->(tag)
RETURN
    friend.id AS personId,
    friend.firstName AS personFirstName,
    friend.lastName AS personLastName,
    count(comment) AS replyCount
ORDER BY
    replyCount DESC,
    personId ASC
LIMIT 20