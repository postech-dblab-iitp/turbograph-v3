MATCH (start:Person {id: 94})<-[:POST_HAS_CREATOR]-(p:Post)<-[:REPLY_OF]-(comment:Comment)-[:HAS_CREATOR]->(person:Person)
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
		LIMIT 20