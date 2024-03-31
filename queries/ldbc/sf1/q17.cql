MATCH (person:Person {id: 30786325583618})-[:KNOWS*2..2]-(friend),
    (friend)-[:IS_LOCATED_IN]->(city:Place {label: "City"})
OPTIONAL MATCH (friend)-[r:KNOWS]-(person)
WHERE NOT friend=person AND r._id = NULL
WITH person, city, friend
WHERE (month(friend.birthday)=11 AND day(friend.birthday)>=21) OR
        (month(friend.birthday)=12 AND day(friend.birthday)<22)
WITH DISTINCT friend, city, person
OPTIONAL MATCH (friend)<-[:POST_HAS_CREATOR]-(post:Post)
WITH friend, city, person, CASE post._id WHEN null THEN 0 ELSE 1 END AS numpost,
    CASE WHEN EXISTS { MATCH (post)-[:POST_HAS_TAG]->(:Tag)<-[:HAS_INTEREST]-(person2:Person) WHERE person2.id = 30786325583618 } THEN 1 ELSE 0 END AS postCommon	
WITH friend, city, person, sum(numpost) AS postCount, sum(postCommon) AS commonPostCount
RETURN friend.id AS personId,
    friend.firstName AS personFirstName,
    friend.lastName AS personLastName,
        commonPostCount - (postCount - commonPostCount) AS commonInterestScore,
        friend.gender AS personGender, 
        city.name AS personCityName
ORDER BY commonInterestScore DESC, personId ASC
LIMIT 10;