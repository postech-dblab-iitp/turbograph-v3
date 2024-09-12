MATCH (v0)-[:`http://purl.org/goodrelations/includes`]->(v1),
(v101)-[:`http://purl.org/goodrelations/offers`]->(v0),
(v1)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v6)
WHERE v101.uri = "http://db.uwaterloo.ca/~galuc/wsdbm/Retailer6" AND 
v0.`http://purl.org/goodrelations/price` IS NOT NULL AND 
v0.`http://purl.org/goodrelations/validThrough` IS NOT NULL AND 
v1.`http://ogp.me/ns#title` IS NOT NULL
RETURN v0, v1, v0.`http://purl.org/goodrelations/price`, v0.`http://purl.org/goodrelations/validThrough`, v1.`http://ogp.me/ns#title`, v6