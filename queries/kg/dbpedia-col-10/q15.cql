MATCH (v1)
WHERE v1.`http://www.w3.org/2000/01/rdf-schema#label` IS NOT NULL OR
(
    v1.`http://www.w3.org/2000/01/rdf-schema#label` IS NOT NULL AND
    (REGEXP_MATCHES(v1.uri, "http://dbpedia.org/resource/.*") OR
    REGEXP_MATCHES(v1.uri, "http://dbpedia.org/ontology/.*") OR
    REGEXP_MATCHES(v1.uri, "http://www.w3.org/2002/07/owl.*") OR
    REGEXP_MATCHES(v1.uri, "http://www.w3.org/2001/XMLSchema.*") OR
    REGEXP_MATCHES(v1.uri, "http://www.w3.org/2000/01/rdf-schema.*") OR
    REGEXP_MATCHES(v1.uri, "http://www.w3.org/1999/02/22-rdf-syntax-ns.*"))
)
RETURN v1.`http://www.w3.org/2000/01/rdf-schema#label`, v1.`http://dbpedia.org/property/largestCity`, v1.`http://dbpedia.org/property/floors`, v1.`http://dbpedia.org/property/shipBuilder`, v1.`http://dbpedia.org/property/allegiance`, v1.`http://dbpedia.org/ontology/MeanOfTransportation/mass`, v1.`http://dbpedia.org/ontology/unitedStatesNationalBridgeId`, v1.`http://dbpedia.org/property/locale`, v1.`http://dbpedia.org/property/depthM`, v1.`http://dbpedia.org/ontology/meanTemperature`, v1.`http://dbpedia.org/property/membership`