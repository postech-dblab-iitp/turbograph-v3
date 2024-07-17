MATCH (n), (m)
WHERE 
n.num < 100 AND
m.num > 58360634
MATCH p = shortestPath((n)-[*]-(m)) 
RETURN n.num, m.num, path_length(p);