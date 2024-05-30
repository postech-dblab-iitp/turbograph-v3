MATCH (person:Person)<-[:HAS_CREATOR]-(comment:Comment)
RETURN 
  person.firstName AS personFirstName,
  person.lastName AS personLastName,
  person.creationDate AS personCreationDate,
  person.birthday AS personBirthday,
  person.locationIP AS personLocationIP,
  comment.creationDate+1 AS commentCreationDate,
  comment.length+1 AS commentLength,
  comment.browserUsed AS commentBrowserUsed,
  comment.locationIP AS commentLocationIP;