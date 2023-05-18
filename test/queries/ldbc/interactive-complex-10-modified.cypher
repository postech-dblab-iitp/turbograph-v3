// Q10. Friend recommendation
/*
:param [{ personId, month }] => { RETURN
  4398046511333 AS personId,
  5 AS month
}
*/
MATCH (person:Person {id: $personId})-[:KNOWS*2..2]-(friend),
       (friend)-[:IS_LOCATED_IN]->(city:City)
WHERE NOT friend=person AND
      NOT EXISTS { (friend)-[:KNOWS]-(person) }
WITH person, city, friend, datetime({epochMillis: friend.birthday}) as birthday
WHERE  (birthday.month=$month AND birthday.day>=21) OR
        (birthday.month=($month%12)+1 AND birthday.day<22)
WITH DISTINCT friend, city, person
OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post:Post)
WITH friend, city, person, post, (CASE WHEN EXISTS { MATCH (post)-[:HAS_TAG]->()<-[:HAS_INTEREST]-(person) } THEN 1 ELSE 0 END) AS postCommon
WITH friend, city, person, count(post) AS postCount, sum(postCommon) AS commonPostCount
RETURN friend.id AS personId,
       friend.firstName AS personFirstName,
       friend.lastName AS personLastName,
       commonPostCount - (postCount - commonPostCount) AS commonInterestScore,
       friend.gender AS personGender,
       city.name AS personCityName
ORDER BY commonInterestScore DESC, personId ASC
LIMIT 10
