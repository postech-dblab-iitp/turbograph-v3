MATCH (person:Person { id: 94 })-[:KNOWS*1..2]->(friend:Person)
WITH DISTINCT friend
MATCH (friend)<-[membership:HAS_MEMBER]-(forum:Forum)
WITH
	forum
OPTIONAL MATCH (otherPerson2:Person)<-[:HAS_CREATOR]-(post:Post)<-[:CONTAINER_OF]-(forum)
WHERE EXISTS {
	MATCH (person:Person { id: $personId })-[:KNOWS*1..2]->(otherPerson2)
	WHERE person <> otherPerson2
}
WITH forum.id as fid, first(forum.title) as fName, count(post.id) AS postCount
RETURN
	fName AS forumName,
	postCount
LIMIT 20