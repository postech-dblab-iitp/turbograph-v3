MATCH (countryX:Place {name: 'Germany' }),
      (countryY:Place {name: 'Hungary' }),
      (person:Person {id: 94 })
WITH person, countryX, countryY
LIMIT 1
MATCH (person)-[:KNOWS*1..2]-(friend:Person)-[:IS_LOCATED_IN]->(city:Place)
WHERE
  NOT person=friend
  AND NOT EXISTS {
    MATCH (city)-[:IS_PART_OF]->(country:Place)
    WHERE country = countryX OR country = countryY
  }
RETURN person;