MATCH (v0)-[:`http://db.uwaterloo.ca/~galuc/wsdbm/hasGenre`]->(v101),
(v4)-[:`http://db.uwaterloo.ca/~galuc/wsdbm/makesPurchase`]->(v5),
(v5)-[:`http://db.uwaterloo.ca/~galuc/wsdbm/purchaseFor`]->(v0)
WHERE v0.`http://schema.org/contentRating` IS NOT NULL AND 
v0.`http://schema.org/contentSize` IS NOT NULL AND 
v101.uri = "http://db.uwaterloo.ca/~galuc/wsdbm/SubGenre69" AND 
v5.`http://db.uwaterloo.ca/~galuc/wsdbm/purchaseDate` IS NOT NULL
RETURN v0, v0.`http://schema.org/contentRating`, v0.`http://schema.org/contentSize`, v4, v5, v5.`http://db.uwaterloo.ca/~galuc/wsdbm/purchaseDate`