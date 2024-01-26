MATCH (person:Person {id: 94})<-[:HAS_CREATOR]-(message:Comment)<-[like:LIKES]-(liker:Person)
WITH liker, message, like, person
ORDER BY like.creationDate DESC, message.id ASC
WITH
	liker,
	person,
	first(message.id) AS msg_id,
	first(like.creationDate) AS likeCreationDate
RETURN
	liker.id AS personId,
	liker.firstName AS personFirstName,
	liker.lastName AS personLastName,
	likeCreationDate AS likeCreationDate,
	msg_id AS commentOrPostId,
	NOT EXISTS { MATCH (liker)-[:KNOWS]-(person) } AS isNew
ORDER BY
	likeCreationDate DESC,
	personId ASC
LIMIT 20