// Q6. Tag co-occurrence
/*
:param [{ personId, tagName }] => { RETURN
  4398046511333 AS personId,
  "Carl_Gustaf_Emil_Mannerheim" AS tagName
}
*/

MATCH (person:Person { id: $personId })-[:KNOWS*1..2]-(friend)
WHERE NOT person=friend
WITH
    distinct friend
    
MATCH (knownTag:Tag { name: $tagName })
WITH friend, knownTag.id as knownTagId
    MATCH (friend)<-[:HAS_CREATOR]-(post:Post),
          (post)-[:HAS_TAG]->(t:Tag{id: knownTagId}),
          (post)-[:HAS_TAG]->(tag:Tag)
    WHERE NOT t = tag
    WITH
        tag.name as tagName,
        count(post) as postCount
RETURN
    tagName,
    postCount
ORDER BY
    postCount DESC,
    tagName ASC
LIMIT 10
