MATCH
(person1:Person {id: 17592186055119}),
(person2:Person {id: 10995116282665})
WITH 
person1, person2
MATCH path = allShortestPaths((person1)-[:KNOWS*1..10]-(person2))
RETURN path_length(path);