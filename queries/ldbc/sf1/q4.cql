MATCH (m:Post {id: 2199029886840})
		RETURN
			m.creationDate as messageCreationDate,
			m.imageFile as messageContent;