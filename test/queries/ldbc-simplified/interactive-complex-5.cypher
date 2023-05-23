MATCH (person:Person { id: 94 })-[:KNOWS*1..2]->(friend:Person)
WITH DISTINCT friend
MATCH (friend)<-[membership:HAS_MEMBER]-(forum:Forum)
WITH
	forum
OPTIONAL MATCH (otherPerson2:Person)<-[:HAS_CREATOR]-(post:Post)<-[:CONTAINER_OF]-(forum)
RETURN
	forum.title AS forumName,
	count(post.id) AS postCount
LIMIT 20