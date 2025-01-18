MATCH
(person1:Person {id: 8796093163356}),
(person2:Person {id: 15393162953088})
WITH 
person1, person2
MATCH path = allShortestPath((person1)-[:KNOWS*1..10]-(person2))
RETURN path