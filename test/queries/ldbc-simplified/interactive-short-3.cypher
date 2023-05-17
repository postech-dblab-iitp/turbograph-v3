MATCH (n:Person {id: 94})-[r:KNOWS]->(friend:Person)
		RETURN
			friend.id AS personId,
			friend.firstName AS firstName,
			friend.lastName AS lastName,
			r.creationDate AS friendshipCreationDate
		ORDER BY
			friendshipCreationDate DESC,
			personId ASC;