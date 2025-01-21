MATCH
    (m:Comment {id: 57174783150815})-[roc:REPLY_OF_COMMENT*0..8]->(n:Comment)-[ro:REPLY_OF]->(p:Post)
    <-[co:CONTAINER_OF]-(f:Forum)-[hm:HAS_MODERATOR]->(mod:Person)
RETURN
    f.id AS forumId,
    f.title AS forumTitle,
    mod.id AS moderatorId,
    mod.firstName AS moderatorFirstName,
    mod.lastName AS moderatorLastName;