MATCH (p:Person {id: $userId})-[:KNOWS]->(friend)-[:KNOWS]->(recommended)
WHERE NOT p = recommended AND
NOT EXISTS {MATCH (p)-[:KNOWS]->(recommended)}
RETURN recommended.id AS recommendedFriend, COUNT(friend) AS mutualFriends
ORDER BY mutualFriends DESC
LIMIT 10