MATCH (v1)
WHERE
v1.`http://dbpedia.org/property/subsid` IS NOT NULL OR
v1.`http://dbpedia.org/property/parent` IS NOT NULL OR
v1.`http://dbpedia.org/property/divisions` IS NOT NULL
RETURN DISTINCT v1.uri, v1.`http://dbpedia.org/property/samtgemeinde`, v1.`http://dbpedia.org/property/launchDate`, v1.`http://dbpedia.org/property/genre`, v1.`http://dbpedia.org/property/headquarters`, v1.`http://dbpedia.org/property/railroadName`