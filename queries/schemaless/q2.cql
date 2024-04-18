MATCH (person:Person)<-[:HAS_CREATOR]-(comment:Comment)-[:REPLY_OF]->(post:Post)
WHERE
  post.creationDate <= 1348012800000
RETURN 
  person.firstName AS personFirstName,
  person.lastName AS personLastName,
  person.creationDate AS personCreationDate,
  person.birthday AS personBirthday,
  person.locationIP AS personLocationIP,
  person.gender AS personGender,
  comment.creationDate AS commentCreationDate,
  comment.content AS commentContent,
  comment.length AS commentLength,
  comment.browserUsed AS commentBrowserUsed,
  comment.locationIP AS commentLocationIP,
  post.imageFile AS postImageFile,
  post.creationDate AS postCreationDate,
  post.content AS postContent,
  post.browserUsed AS postBrowserUsed,
  post.language AS postLanguage;