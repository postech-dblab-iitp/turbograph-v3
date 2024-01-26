MATCH (person:Person)<-[:KNOWS]-(friend:Person)<-[:HAS_CREATOR]-(comment:Comment)-[:REPLY_OF]->(post:Post)-[:POST_HAS_TAG]->(tag:Tag)
WHERE EXISTS {
  MATCH (innerTag:Tag)-[:HAS_TYPE]->(baseTagClass:TagClass)
  WHERE
  	(innerTag.name = 'Hamid_Karzai' OR baseTagClass.name = 'Hamid_Karzai')
  	AND tag.id = innerTag.id
}
RETURN
	friend.id AS personId,
	friend.firstName AS personFirstName,
	friend.lastName AS personLastName,
	count(comment) AS replyCount
ORDER BY
	replyCount DESC,
	personId ASC
LIMIT 20