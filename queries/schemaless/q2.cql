MATCH (n:Person {id: 32985348864506 })-[:KNOWS]->(friend:Person)<-[:HAS_CREATOR]-(message:Comment)
		WHERE message.creationDate <= 1348012800000
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