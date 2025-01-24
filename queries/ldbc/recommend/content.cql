MATCH (p:Person {id: $userId})-[:KNOWS]->(friend)
MATCH (friend)-[:LIKES_POST]->(post:Post)
WHERE NOT EXISTS {MATCH (p)-[:LIKES_POST]->(post)}
RETURN post.id AS recommendedPost, COUNT(friend) AS friendLikes
ORDER BY friendLikes DESC
LIMIT 10
