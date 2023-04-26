// Q5. New groups
/*
:param [{ personId, minDate }] => { RETURN
  6597069766734 AS personId,
  1288612800000 AS minDate
}
*/
MATCH (person:Person { id: $personId })-[:KNOWS*1..2]-(otherPerson)
WHERE
    person <> otherPerson
WITH DISTINCT otherPerson
MATCH (otherPerson)<-[membership:HAS_MEMBER]-(forum)
WHERE
    membership.creationDate > $minDate
WITH
    forum
OPTIONAL MATCH (otherPerson2)<-[:HAS_CREATOR]-(post)<-[:CONTAINER_OF]-(forum)
WHERE EXISTS {
        MATCH (person:Person { id: $personId })-[:KNOWS*1..2]-(p)
        WHERE person <> p
    }
WITH
    forum,
    count(post) AS postCount
RETURN
    forum.title AS forumName,
    postCount
ORDER BY
    postCount DESC,
    forum.id ASC
LIMIT 20
