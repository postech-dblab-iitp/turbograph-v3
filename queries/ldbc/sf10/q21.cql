MATCH
(person1:Person {id: 32985348886934}),
(person2:Person {id: 15393162803709})
WITH 
person1, person2
MATCH path = allShortestPath((person1)-[:KNOWS*1..10]-(person2))
RETURN CASE path
        WHEN null THEN 0
        ELSE path_length(path)
    END AS shortestPathLength;