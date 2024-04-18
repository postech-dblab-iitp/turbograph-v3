MATCH (person:Person)<-[:HAS_CREATOR]-(comment:Comment)-[:REPLY_OF]->(post:Post)
RETURN 
  person.id AS personId,
  person.creationDate AS personCreationDate,
  post.creationDate AS postCreationDate,
  comment.creationDate AS commentCreationDate;