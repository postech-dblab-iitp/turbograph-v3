MATCH (person:Person {id: 94 })-[:KNOWS]-(friend:Person),
      	(friend)<-[:POST_HAS_CREATOR]-(post:Post)-[:POST_HAS_TAG]->(tag:Tag)
		WITH DISTINCT tag, post
		WITH tag,
			CASE
			WHEN post.creationDate >= 1275350400000 THEN 1
			ELSE 0
			END AS valid,
			CASE
			WHEN post.creationDate < 1275350400000 THEN 1
			ELSE 0
			END AS inValid
		WITH tag.id AS tagid, tag.name AS tagName, sum(valid) AS postCount, sum(inValid) AS inValidPostCount
		WHERE postCount>0
		RETURN tagName
		ORDER BY tagName ASC
		LIMIT 10