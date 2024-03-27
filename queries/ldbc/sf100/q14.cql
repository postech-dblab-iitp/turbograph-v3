MATCH (person:Person {id: 30786325617380})<-[:HAS_CREATOR]-(message:Comment)<-[like:LIKES]-(liker:Person)
WITH liker, message, like.creationDate AS likeTime, person
ORDER BY likeTime DESC, message.id ASC
WITH
    liker,
    first(message.id) AS msg_id,
    first(likeTime) AS likeCreationDate,
    first(message.content) AS commentOrPostContent,
    first(message.creationDate) AS m_creationDate,
    person
RETURN
    liker.id AS personId,
    liker.firstName AS personFirstName,
    liker.lastName AS personLastName,
    msg_id,
    likeCreationDate,
    commentOrPostContent,
    (((likeCreationDate - m_creationDate)/1000.0)/60.0) AS minutesLatency,
    CASE WHEN EXISTS { MATCH (liker)-[:KNOWS]-(person) } THEN false ELSE true END AS isNew
ORDER BY
    likeCreationDate DESC,
    personId ASC
LIMIT 20;