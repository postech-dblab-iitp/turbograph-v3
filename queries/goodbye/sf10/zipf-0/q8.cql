MATCH (n {num: 0}), (m {num: 3981023})
MATCH p = shortestPath((n)-[*]-(m)) 
RETURN path_length(p);