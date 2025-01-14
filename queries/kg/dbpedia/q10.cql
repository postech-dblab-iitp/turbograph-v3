MATCH (v1)-[:`http://xmlns.com/foaf/0.1/page`]->(v2)
WHERE v1.`http://www.w3.org/2000/01/rdf-schema#comment` IS NOT NULL
OPTIONAL MATCH (v1)-[:`http://purl.org/dc/terms/subject`]->(v3)
OPTIONAL MATCH (v1)-[:`http://dbpedia.org/property/industry`]->(v5)
OPTIONAL MATCH (v1)-[:`http://dbpedia.org/property/location`]->(v6)
OPTIONAL MATCH (v1)-[:`http://dbpedia.org/property/locationCountry`]->(v7)
OPTIONAL MATCH (v1)-[:`http://dbpedia.org/property/locationCity`]->(v8), 
(v1)-[:`http://dbpedia.org/property/manufacturer`]->(v10)
OPTIONAL MATCH (v1)-[:`http://dbpedia.org/property/products`]->(v9)
OPTIONAL MATCH (v1)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v11)
RETURN DISTINCT v2.uri
LIMIT 1000;
