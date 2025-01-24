MATCH (p:Person {id: $userId})-[:IS_LOCATED_IN]->(location)
MATCH (recommended:Person)-[:IS_LOCATED_IN]->(location)
WHERE NOT p = recommended AND
NOT EXISTS {MATCH (p)-[:KNOWS]->(recommended)}
RETURN recommended.id AS recommendedFriend, location.name AS sharedLocation
LIMIT 10
