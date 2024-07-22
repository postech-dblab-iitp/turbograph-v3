MATCH (n), (m)
WHERE 
n.num = 0 AND
m.num > 58370534
MATCH p = shortestPath((n)-[*]-(m)) 
RETURN n.num, m.num, path_length(p);