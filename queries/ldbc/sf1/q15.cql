MATCH (start:Person {id: 24189255818757})<-[:HAS_CREATOR]-(p:Comment)<-[:REPLY_OF_COMMENT]-(comment:Comment)-[:HAS_CREATOR]->(person:Person)
RETURN
    person.id AS personId,
    person.firstName AS personFirstName,
    person.lastName AS personLastName,
    comment.creationDate AS commentCreationDate,
    comment.id AS commentId,
    comment.content AS commentContent
ORDER BY
    commentCreationDate DESC,
    commentId ASC
LIMIT 20;