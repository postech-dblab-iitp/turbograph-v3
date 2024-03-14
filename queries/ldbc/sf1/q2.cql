MATCH (:Person {id: 933})<-[:HAS_CREATOR]-(message:Message)
		WITH
			message,
			message.id AS messageId,
			message.creationDate AS messageCreationDate
		ORDER BY messageCreationDate DESC, messageId ASC
		LIMIT 10
		MATCH (message)-[:REPLY_OF_COMMENT*0..8]->(n:Comment)-[ro:REPLY_OF]->(post:Post),
			(post)-[:POST_HAS_CREATOR]->(person:Person)
		RETURN
			messageId,
			message.content AS messageContent,
			messageCreationDate,
			post.id AS postId,
			person.id AS personId,
			person.firstName AS personFirstName,
			person.lastName AS personLastName
		ORDER BY messageCreationDate DESC, messageId ASC;

MATCH (message:Comment) RETURN message.id;
MATCH (message:Message) RETURN message.id;