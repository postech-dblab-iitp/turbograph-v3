MATCH (n), (m)
WHERE 
n.num = 4 AND 
m.num > 7950233
MATCH p = shortestPath((n)-[*]-(m)) 
RETURN n.num, m.num, path_length(p);