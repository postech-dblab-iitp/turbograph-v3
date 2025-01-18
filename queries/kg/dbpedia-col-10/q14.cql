MATCH (v1)
WHERE v1.`http://www.w3.org/2000/01/rdf-schema#label` IS NOT NULL OR
v1.`http://dbpedia.org/ontology/abstract` IS NOT NULL OR
v1.`http://www.w3.org/2000/01/rdf-schema#comment` IS NOT NULL OR
(
    v1.`http://dbpedia.org/ontology/abstract` IS NULL AND
    v1.`http://www.w3.org/2000/01/rdf-schema#comment` IS NULL AND
    v1.`http://www.w3.org/2000/01/rdf-schema#label` IS NULL
)
RETURN v1.uri, v1.`http://dbpedia.org/property/dischargeAverage`, v1.`http://dbpedia.org/ontology/pseudonym`, v1.`http://dbpedia.org/property/name`, v1.`http://dbpedia.org/property/valvetrain`, v1.`http://dbpedia.org/property/brgermeister`, v1.`http://dbpedia.org/property/orbits`, v1.`http://dbpedia.org/property/resSurface`, v1.`http://dbpedia.org/property/mascot`, v1.`http://dbpedia.org/property/areaAcre`, v1.`http://dbpedia.org/ontology/areaRural`