MATCH (m:Post {id: 8246343738160})-[r:POST_HAS_CREATOR]->(p:Person)
		RETURN
			p.id AS personId,
			p.firstName AS firstName,
			p.lastName AS lastName;