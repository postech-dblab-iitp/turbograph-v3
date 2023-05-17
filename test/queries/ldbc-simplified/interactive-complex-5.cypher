MATCH (person:Person { id: 94 })-[:KNOWS*1..2]->(friend:Person)
		WITH DISTINCT friend
		MATCH (friend)<-[membership:HAS_MEMBER]-(forum:Forum)
		WITH
			forum,
			friend
		MATCH (post:Post)<-[:CONTAINER_OF]-(forum)
		WITH
			forum, post
		RETURN
			forum.title AS forumName,
			count(post.id) AS postCount
		LIMIT 20