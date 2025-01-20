MATCH 
  (music)-[:`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`]->(type),       (music)-[:`http://dbpedia.org/ontology/genre`]->(subgenre)<-[:`http://dbpedia.org/ontology/musicSubgenre`]-(genre)<-[:`http://dbpedia.org/ontology/stylisticOrigin`]-(style)
WHERE 
  type.uri = "http://dbpedia.org/ontology/MusicalWork" AND
  style.uri = "http://dbpedia.org/resource/Rhythm_and_blues"
RETURN 
  subgenre.`http://www.w3.org/2000/01/rdf-schema#label` AS subgenre_label,
  music.`http://dbpedia.org/ontology/releaseDate` AS release_date,
  COUNT(music) AS total_music
ORDER BY 
  release_date DESC
