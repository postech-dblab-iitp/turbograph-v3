MATCH (v101)-[:`http://www.geonames.org/ontology#parentCountry`]->(v1),
(v2)-[:`http://db.uwaterloo.ca/~galuc/wsdbm/likes`]->(v102),
(v2)-[:`http://schema.org/nationality`]->(v1)
WHERE v101.uri = "http://db.uwaterloo.ca/~galuc/wsdbm/City121" AND 
v102.uri = "http://db.uwaterloo.ca/~galuc/wsdbm/Product0"
RETURN v1, v2