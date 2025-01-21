MATCH (m:Comment {id: 824635044682 })<-[:REPLY_OF_COMMENT]-(c:Comment)-[:HAS_CREATOR]->(p:Person)
	    OPTIONAL MATCH (m)-[:HAS_CREATOR]->(a:Person)<-[r:KNOWS]-(p)
	    RETURN c.id AS commentId,
			c.content AS commentContent,
			c.creationDate AS commentCreationDate,
			p.id AS replyAuthorId,
			p.firstName AS replyAuthorFirstName,
			p.lastName AS replyAuthorLastName,
		CASE r._id
		    WHEN null THEN false
		    ELSE true
		END AS replyAuthorKnowsOriginalMessageAuthor
	    ORDER BY commentCreationDate DESC, replyAuthorId;