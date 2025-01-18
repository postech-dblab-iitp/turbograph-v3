MATCH (v1)
WHERE
v1.`http://dbpedia.org/property/subsid` IS NOT NULL OR
v1.`http://dbpedia.org/property/parent` IS NOT NULL OR
v1.`http://dbpedia.org/property/divisions` IS NOT NULL
RETURN DISTINCT v1.uri, v1.`http://dbpedia.org/property/share`, v1.`http://dbpedia.org/property/keyPeople`, v1.`http://dbpedia.org/property/equity`, v1.`http://dbpedia.org/property/region`, v1.`http://dbpedia.org/property/lastAired`, v1.`http://dbpedia.org/property/operatingIncome`, v1.`http://dbpedia.org/property/operatingSystem`, v1.`http://dbpedia.org/property/enddate`, v1.`http://dbpedia.org/ontology/numberOfVehicles`, v1.`http://dbpedia.org/ontology/equity`