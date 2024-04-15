MATCH (person:Person)<-[:HAS_CREATOR]-(comment:Comment)-[:REPLY_OF]->(post:Post)
WHERE
  person.creationDate < 1266606692284
RETURN 
  person.id AS personId,
  person.creationDate AS personCreationDate,
  person.birthday AS personBirthday,
  post.browserUsed AS postBrowserUsed,
  post.creationDate AS postCreationDate,
  comment.creationDate AS commentCreationDate,
  comment.browserUsed AS commentBrowserUsed;