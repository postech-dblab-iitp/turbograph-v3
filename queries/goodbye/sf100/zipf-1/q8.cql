MATCH (n), (m)
WHERE 
n.num < 100 AND
m.num > 7940333
MATCH p = shortestPath((n)-[*]-(m)) 
RETURN n.num, m.num, path_length(p);