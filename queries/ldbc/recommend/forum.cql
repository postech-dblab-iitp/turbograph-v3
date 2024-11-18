MATCH (p:Person {id: $userId})<-[:POST_HAS_CREATOR]-(content)-[:POST_HAS_TAG]->(tag)
MATCH (forum:Forum)-[:FORUM_HAS_TAG]->(tag)
WHERE NOT EXISTS {MATCH (p)<-[:POST_HAS_CREATOR]-(:Post)-[:POST_HAS_TAG]->(tag)<-[:FORUM_HAS_TAG]-(forum)}
RETURN forum.id AS recommendedForum, COUNT(DISTINCT tag) AS sharedTags
ORDER BY sharedTags DESC
LIMIT 5
