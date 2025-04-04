MATCH (v1)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(v2)
RETURN DISTINCT v1.uri AS r1, v1.`http://dbpedia.org/property/gross`, v1.`http://dbpedia.org/property/formercoach`, v1.`http://dbpedia.org/ontology/settingOfPlay`, v1.`http://dbpedia.org/property/occupation`, v1.`http://dbpedia.org/ontology/Work/runtime`
