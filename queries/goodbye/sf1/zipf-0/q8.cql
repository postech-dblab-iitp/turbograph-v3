MATCH (n {num: 0}), (m {num: 398926})
MATCH p = shortestPath((n)-[*]-(m)) 
RETURN path_length(p);