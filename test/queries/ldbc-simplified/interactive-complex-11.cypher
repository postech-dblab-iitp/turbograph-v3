MATCH (person:Person {id: 10995116277918 })-[:KNOWS*1..2]-(friend:Person)
WITH DISTINCT friend
MATCH (friend)-[workAt:WORK_AT]->(company:Organisation {label: 'Company'})-[:ORG_IS_LOCATED_IN]->(:Place {name: 'Hungary' })
RETURN
		friend.id AS personId,
		friend.firstName AS personFirstName,
		friend.lastName AS personLastName,
		company.name AS organizationName,
		workAt.workFrom AS organizationWorkFromYear
ORDER BY
		organizationWorkFromYear ASC,
		personId ASC,
		organizationName DESC
LIMIT 10