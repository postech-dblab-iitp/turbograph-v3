MATCH (person:Person)<-[:HAS_CREATOR]-(comment:Comment)-[:REPLY_OF]->(post:Post)-[:POST_HAS_CREATOR]->(person2:Person)
RETURN 
  person.creationDate AS personCreationDate,
  person.birthday AS personBirthday,
  person.locationIP AS personLocationIP,
  comment.creationDate AS commentCreationDate,
  comment.length AS commentLength,
  comment.browserUsed AS commentBrowserUsed,
  comment.locationIP AS commentLocationIP,
  post.creationDate AS postCreationDate,
  post.browserUsed AS postBrowserUsed,
  post.language AS postLanguage,
  person2.creationDate AS person2CreationDate,
  person2.birthday AS person2Birthday,
  person2.locationIP AS person2Location;