// Q12. Expert search
/*
:param [{ personId, tagClassName }] => { RETURN
  10995116278009 AS personId,
  "Monarch" AS tagClassName
}
*/
MATCH (person:Person)<-[:KNOWS]-(friend:Person)<-[:HAS_CREATOR]-(comment:Comment)-[:REPLY_OF]->(post:Post)-[:POST_HAS_TAG]->(tag:Tag)
WHERE EXISTS {
  MATCH (innerTag:Tag)-[:HAS_TYPE]->(baseTagClass:TagClass)
  WHERE
  	(innerTag.name = $tagClassName OR baseTagClass.name = $tagClassName)
  	AND tag.id = innerTag.id
}
RETURN
    friend.id AS personId,
    friend.firstName AS personFirstName,
    friend.lastName AS personLastName,
    collect(DISTINCT tag.name) AS tagNames,
    count(DISTINCT comment) AS replyCount
ORDER BY
    replyCount DESC,
    toInteger(personId) ASC
LIMIT 20
