MATCH (v101)-[:`http://www.geonames.org/ontology#parentCountry`]->(v3),
(v0)-[:`http://schema.org/nationality`]->(v3)
WHERE v0.`http://schema.org/jobTitle` IS NOT NULL AND 
v101.uri = "http://db.uwaterloo.ca/~galuc/wsdbm/City121"
RETURN v0, v0.`http://schema.org/jobTitle`, v3