MATCH (m:Comment {id: 57174783150815})-[r:HAS_CREATOR]->(p:Person)
		RETURN
			p.id AS personId,
			p.firstName AS firstName,
			p.lastName AS lastName;