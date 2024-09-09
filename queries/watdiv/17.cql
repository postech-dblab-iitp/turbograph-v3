MATCH (v0)-[:`http://xmlns.com/foaf/age`]->(v101),
(v3)-[:`http://purl.org/ontology/mo/artist`]->(v0),
(v0)-[:`http://schema.org/nationality`]->(v102)
WHERE v101.uri = "http://db.uwaterloo.ca/~galuc/wsdbm/Website36" AND 
v0.`http://xmlns.com/foaf/familyName` IS NOT NULL AND 
v102.uri = "http://db.uwaterloo.ca/~galuc/wsdbm/Country1"
RETURN v0.id, v0.`http://xmlns.com/foaf/familyName`, v3.id