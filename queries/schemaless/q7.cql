MATCH (person:Person)<-[:HAS_CREATOR]-(comment:Comment)-[:REPLY_OF]->(post:Post)
RETURN 
  person.creationDate AS personCreationDate,
  person.birthday AS personBirthday,
  post.id AS postId,
  post.creationDate AS postCreationDate,
  comment.id AS commentId,
  comment.creationDate AS commentCreationDate,
  comment.browserUsed AS commentBrowserUsed;