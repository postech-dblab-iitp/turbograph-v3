MATCH (countryX:Place {name: "Honduras", label: "Country"}),
    (countryY:Place {name: "Hong_Kong", label: "Country"}),
    (person:Person {id: 28587302374257 })
WITH person, countryX, countryY
LIMIT 1
MATCH (city:Place {label: "City"})-[:IS_PART_OF]->(country:Place {label: "Country"})
WHERE country._id = countryX._id OR country._id = countryY._id
WITH person, countryX, countryY, collect(city._id) AS cities
MATCH (person)-[:KNOWS*1..2]-(friend:Person)-[:IS_LOCATED_IN]->(city2:Place)
WHERE NOT person=friend AND NOT city2._id in cities
WITH DISTINCT friend, countryX, countryY, city2.id AS cid
MATCH (friend)<-[:POST_HAS_CREATOR]-(message:Post)
WHERE 1301702400000 > message.creationDate AND message.creationDate >= 1298937600000
WITH friend, countryX, countryY, message
MATCH (message)-[:POST_IS_LOCATED_IN]->(country2:Place)
WHERE country2._id = countryX._id OR country2._id = countryY._id
WITH friend.id AS friendId, friend.firstName AS friendFirstName, friend.lastName AS friendLastName,
    CASE WHEN country2=countryX THEN 1 ELSE 0 END AS messageX,
    CASE WHEN country2=countryY THEN 1 ELSE 0 END AS messageY
WITH friendId, friendFirstName, friendLastName,
    sum(messageX) AS xCount, sum(messageY) AS yCount
WHERE xCount>0 AND yCount>0
RETURN friendId,
    friendFirstName,
    friendLastName,
    xCount,
    yCount,
    xCount + yCount AS xyCount
ORDER BY xyCount DESC, friendId ASC
LIMIT 20;