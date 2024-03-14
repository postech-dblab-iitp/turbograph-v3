MATCH (m:Comment {id: 8246343738160})-[r:HAS_CREATOR]->(p:Person)
		RETURN
			p.id AS personId,
			p.firstName AS firstName,
			p.lastName AS lastName;