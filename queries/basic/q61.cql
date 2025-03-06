MATCH (n:Person)-[:KNOWS]->(m0:Person)-[:KNOWS]->(m1:Person)-[:KNOWS]->(m2:Person) RETURN n, m0, m1, m2;
