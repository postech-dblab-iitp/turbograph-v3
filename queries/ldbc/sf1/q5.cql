MATCH (m:Comment {id: 824635044686})-[r:HAS_CREATOR]->(p:Person)
		RETURN
			p.id AS personId,
			p.firstName AS firstName,
			p.lastName AS lastName;