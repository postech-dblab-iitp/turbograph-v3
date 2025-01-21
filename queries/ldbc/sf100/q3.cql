MATCH (n:Person {id: 28587302462299})-[r:KNOWS]->(friend:Person)
		RETURN
			friend.id AS personId,
			friend.firstName AS firstName,
			friend.lastName AS lastName,
			r.creationDate AS friendshipCreationDate
		ORDER BY
			friendshipCreationDate DESC,
			personId ASC;