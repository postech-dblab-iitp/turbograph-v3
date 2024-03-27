MATCH
(person1:Person {id: 8796093163356}),
(person2:Person {id: 4398046593165})
WITH 
person1, person2
MATCH path = shortestPath((person1)-[:KNOWS*]-(person2))
RETURN CASE path
        WHEN null THEN 0
        ELSE path_length(path)
    END AS shortestPathLength