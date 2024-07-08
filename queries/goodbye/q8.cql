MATCH (n {num: 0}), (m {num: 101629})
MATCH p = shortestPath((n)-[*]-(m)) 
RETURN path_length(p);