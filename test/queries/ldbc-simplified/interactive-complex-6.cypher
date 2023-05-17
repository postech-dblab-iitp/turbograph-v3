MATCH (knownTag:Tag { name: 'Carl_Gustaf_Emil_Mannerheim' })
		WITH knownTag.id as knownTagId
		MATCH (person:Person { id: 4398046511333 })
		RETURN person.id, knownTagId