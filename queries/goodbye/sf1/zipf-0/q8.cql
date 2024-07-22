MATCH (n), (m)
WHERE 
n.num = 10 AND 
m.num > 795054
MATCH p = shortestPath((n)-[*]-(m)) 
RETURN n.num, m.num, path_length(p);