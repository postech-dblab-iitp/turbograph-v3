MATCH (person:Person)<-[:HAS_CREATOR]-(comment:Comment)
WHERE comment.length < 20
  AND comment.creationDate > 1266606692284
MATCH (comment)-[:REPLY_OF]->(post:Post)
RETURN 
  person.id AS personId,
  person.locationIP AS personLocationIP,
  person.gender AS personGender,
  post.creationDate AS postCreationDate,
  post.language AS postLanguage