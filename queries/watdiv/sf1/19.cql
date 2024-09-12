MATCH (v0)-[:`http://purl.org/ontology/mo/conductor`]->(v1),
(v0)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v2),
(v0)-[:`http://db.uwaterloo.ca/~galuc/wsdbm/hasGenre`]->(v101)
WHERE v101.uri = "http://db.uwaterloo.ca/~galuc/wsdbm/SubGenre95"
RETURN v0, v1, v2