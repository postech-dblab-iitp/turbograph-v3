MATCH
(person1:Person {id: 32985348886934}),
(person2:Person {id: 15393162823425})
WITH 
person1, person2
MATCH path = shortestPath((person1)-[:KNOWS*]-(person2))
RETURN CASE path
        WHEN null THEN 0
        ELSE path_length(path)
    END AS shortestPathLength