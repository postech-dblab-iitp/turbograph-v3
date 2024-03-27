MATCH (m:Post {id: 39582418599937})
		RETURN
			m.creationDate as messageCreationDate,
			m.imageFile as messageContent;