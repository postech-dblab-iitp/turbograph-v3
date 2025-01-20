MATCH (n:Person {id: 2199023521857 })-[:KNOWS]->(friend:Person)<-[:POST_HAS_CREATOR]-(message:Post)
		WHERE message.creationDate <= 1313798400000
		RETURN
			friend.id AS personId,
			friend.firstName AS personFirstName,
			friend.lastName AS personLastName,
			message.id AS postOrCommentId,
			message.content AS postOrCommentContent,
			message.creationDate AS postOrCommentCreationDate
		ORDER BY
			postOrCommentCreationDate DESC,
			postOrCommentId ASC
		LIMIT 20;