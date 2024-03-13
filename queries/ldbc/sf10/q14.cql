MATCH (person:Person {id: 2199023278337})<-[:HAS_CREATOR]-(message:Comment)<-[like:LIKES]-(liker:Person)
WITH liker, message, like.creationDate AS likeTime, person
ORDER BY likeTime DESC, message.id ASC
WITH liker, message, person, likeTime,
    CASE WHEN EXISTS { MATCH (liker)-[:KNOWS]-(person) } THEN 0 ELSE 1 END AS isNew
RETURN
    liker.id AS personId,
    liker.firstName AS personFirstName,
    liker.lastName AS personLastName,
    first(message.id) AS msg_id,
    first(likeTime) AS likeCreationDate,
    message.content AS commentOrPostContent,
    isNew
ORDER BY
    likeCreationDate DESC,
    personId ASC
LIMIT 20;