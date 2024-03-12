MATCH (knownTag:Tag { name: 'Angola' })
		WITH knownTag.id as knownTagId
		MATCH (person:Person { id: 30786325583618 })-[:KNOWS*1..2]-(friend)
		WHERE NOT person=friend
		WITH distinct friend, knownTagId
		MATCH (friend)<-[:POST_HAS_CREATOR]-(post:Post),
			(post)-[:POST_HAS_TAG]->(t:Tag{id: knownTagId}),
			(post)-[:POST_HAS_TAG]->(tag:Tag)
		WHERE NOT t = tag
		WITH
			tag.name as tagName,
			count(post) as postCount
		RETURN
			tagName,
			postCount
		ORDER BY
			postCount DESC,
			tagName ASC
		LIMIT 10;