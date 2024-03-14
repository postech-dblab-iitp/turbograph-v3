MATCH (m:Post {id: 8796133549184})
		RETURN
			m.creationDate as messageCreationDate,
			m.imageFile as messageContent;