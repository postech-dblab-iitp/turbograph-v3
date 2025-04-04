MATCH (v1)
WHERE
v1.`http://dbpedia.org/property/subsid` IS NOT NULL OR
v1.`http://dbpedia.org/property/parent` IS NOT NULL OR
v1.`http://dbpedia.org/property/divisions` IS NOT NULL
RETURN DISTINCT v1.uri, v1.`http://dbpedia.org/property/url`, v1.`http://dbpedia.org/ontology/slogan`, v1.`http://dbpedia.org/ontology/trackLength`, v1.`http://dbpedia.org/property/hqCity`, v1.`http://dbpedia.org/property/name`