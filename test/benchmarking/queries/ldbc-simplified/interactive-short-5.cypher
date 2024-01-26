MATCH (m:Comment {id: 557})-[r:HAS_CREATOR]->(p:Person)
		RETURN
			p.id AS personId,
			p.firstName AS firstName,
			p.lastName AS lastName;