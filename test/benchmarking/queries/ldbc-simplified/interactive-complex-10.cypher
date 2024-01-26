MATCH (person:Person {id: 94})-[:KNOWS]-(friend:Person), (friend)-[:IS_LOCATED_IN]->(city:Place)
WHERE NOT friend=person AND NOT EXISTS { MATCH (friend)-[:KNOWS]-(person) }
RETURN person
LIMIT 1