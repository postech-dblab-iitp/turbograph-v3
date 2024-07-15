MATCH (n), (m)
WHERE n.num < 10000 AND m.num <= 10
MATCH p = shortestPath((n)-[*]-(m)) 
RETURN n.num, m.num, path_length(p);