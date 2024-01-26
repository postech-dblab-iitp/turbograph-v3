MATCH (m:Comment {id: 557})
		RETURN
			m.creationDate as messageCreationDate,
			m.content as messageContent;