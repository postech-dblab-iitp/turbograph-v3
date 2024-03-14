MATCH (n:Person {id: 32985348897268})-[r:KNOWS]->(friend:Person)
		RETURN
			friend.id AS personId,
			friend.firstName AS firstName,
			friend.lastName AS lastName,
			r.creationDate AS friendshipCreationDate
		ORDER BY
			friendshipCreationDate DESC,
			personId ASC;