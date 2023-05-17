MATCH (person:Person {id: 94})<-[:HAS_CREATOR]-(message:Comment)<-[like:LIKES]-(liker:Person)
		WITH liker, message, like, person
		ORDER BY like.creationDate DESC, message.id ASC
		RETURN
			liker.id AS personId,
			liker.firstName AS personFirstName,
			liker.lastName AS personLastName,
			first(message.id) AS msg_id,
			first(like.creationDate) AS likeCreationDate,
			person._id AS person_id
		ORDER BY
			likeCreationDate DESC,
			personId ASC
		LIMIT 20