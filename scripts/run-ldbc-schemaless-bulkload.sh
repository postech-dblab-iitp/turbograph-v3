#!/bin/bash
SF=1
basedir="/source-data/ldbc-hadoop/"
intermdir="/converted/"
#"/graphs/csv/interactive/composite-projected-fk/"

./tbgpp-execution-engine/bulkload_using_map \
	--output_dir:"/data/ldbc/sf1_schemaless" \
	--jsonl:"--file_path:/source-data/schemaless/ldbc/dynamic/person/Person_10.json --nodes:Person" \
	--jsonl:"--file_path:/source-data/schemaless/ldbc/dynamic/comment/Comment_10.json --nodes:Comment:Message" \
	--jsonl:"--file_path:/source-data/schemaless/ldbc/dynamic/post/Post_10.json --nodes:Post:Message" \
	--nodes:Forum ${basedir}/sf${SF}/${intermdir}/dynamic/Forum.csv \
	--nodes:Organisation ${basedir}/sf${SF}/${intermdir}/static/Organisation.csv \
	--nodes:Place ${basedir}/sf${SF}/${intermdir}/static/Place.csv \
	--nodes:Tag ${basedir}/sf${SF}/${intermdir}/static/Tag.csv \
	--nodes:TagClass ${basedir}/sf${SF}/${intermdir}/static/TagClass.csv \
	--relationships:HAS_CREATOR ${basedir}/sf${SF}/${intermdir}/dynamic/Comment_hasCreator_Person.csv \
	--relationships:POST_HAS_CREATOR ${basedir}/sf${SF}/${intermdir}/dynamic/Post_hasCreator_Person.csv \
	--relationships:IS_LOCATED_IN ${basedir}/sf${SF}/${intermdir}/dynamic/Person_isLocatedIn_Place.csv \
	--relationships:KNOWS ${basedir}/sf${SF}/${intermdir}/dynamic/Person_knows_Person.csv \
	--relationships:LIKES ${basedir}/sf${SF}/${intermdir}/dynamic/Person_likes_Comment.csv \
	--relationships:LIKES_POST ${basedir}/sf${SF}/${intermdir}/dynamic/Person_likes_Post.csv \
	--relationships:HAS_INTEREST ${basedir}/sf${SF}/${intermdir}/dynamic/Person_hasInterest_Tag.csv \
	--relationships:STUDY_AT ${basedir}/sf${SF}/${intermdir}/dynamic/Person_studyAt_Organisation.csv \
	--relationships:REPLY_OF ${basedir}/sf${SF}/${intermdir}/dynamic/Comment_replyOf_Post.csv \
	--relationships:REPLY_OF_COMMENT ${basedir}/sf${SF}/${intermdir}/dynamic/Comment_replyOf_Comment.csv \
	--relationships:COMMENT_IS_LOCATED_IN ${basedir}/sf${SF}/${intermdir}/dynamic/Comment_isLocatedIn_Place.csv \
	--relationships:HAS_TAG ${basedir}/sf${SF}/${intermdir}/dynamic/Comment_hasTag_Tag.csv \
	--relationships:CONTAINER_OF ${basedir}/sf${SF}/${intermdir}/dynamic/Forum_containerOf_Post.csv \
	--relationships:HAS_MODERATOR ${basedir}/sf${SF}/${intermdir}/dynamic/Forum_hasModerator_Person.csv \
	--relationships:HAS_MEMBER ${basedir}/sf${SF}/${intermdir}/dynamic/Forum_hasMember_Person.csv \
	--relationships:FORUM_HAS_TAG ${basedir}/sf${SF}/${intermdir}/dynamic/Forum_hasTag_Tag.csv \
	--relationships:POST_HAS_TAG ${basedir}/sf${SF}/${intermdir}/dynamic/Post_hasTag_Tag.csv \
	--relationships:POST_IS_LOCATED_IN ${basedir}/sf${SF}/${intermdir}/dynamic/Post_isLocatedIn_Place.csv \
	--relationships:WORK_AT ${basedir}/sf${SF}/${intermdir}/dynamic/Person_workAt_Organisation.csv \
	--relationships:ORG_IS_LOCATED_IN ${basedir}/sf${SF}/${intermdir}/static/Organisation_isLocatedIn_Place.csv \
	--relationships:IS_PART_OF ${basedir}/sf${SF}/${intermdir}/static/Place_isPartOf_Place.csv \
	--relationships:IS_SUBCLASS_OF ${basedir}/sf${SF}/${intermdir}/static/TagClass_isSubclassOf_TagClass.csv \
	--relationships:HAS_TYPE ${basedir}/sf${SF}/${intermdir}/static/Tag_hasType_TagClass.csv
	# --relationships_backward:HAS_CREATOR ${basedir}/sf${SF}/${intermdir}/dynamic/Comment_hasCreator_Person.csv.backward \
	# --relationships_backward:POST_HAS_CREATOR ${basedir}/sf${SF}/${intermdir}/dynamic/Post_hasCreator_Person.csv.backward \
	# --relationships_backward:IS_LOCATED_IN ${basedir}/sf${SF}/${intermdir}/dynamic/Person_isLocatedIn_Place.csv.backward \
	# --relationships_backward:KNOWS ${basedir}/sf${SF}/${intermdir}/dynamic/Person_knows_Person.csv.backward \
	# --relationships_backward:LIKES ${basedir}/sf${SF}/${intermdir}/dynamic/Person_likes_Comment.csv.backward \
	# --relationships_backward:LIKES_POST ${basedir}/sf${SF}/${intermdir}/dynamic/Person_likes_Post.csv.backward \
	# --relationships_backward:HAS_INTEREST ${basedir}/sf${SF}/${intermdir}/dynamic/Person_hasInterest_Tag.csv.backward \
	# --relationships_backward:STUDY_AT ${basedir}/sf${SF}/${intermdir}/dynamic/Person_studyAt_Organisation.csv.backward \
	# --relationships_backward:REPLY_OF ${basedir}/sf${SF}/${intermdir}/dynamic/Comment_replyOf_Post.csv.backward \
	# --relationships_backward:REPLY_OF_COMMENT ${basedir}/sf${SF}/${intermdir}/dynamic/Comment_replyOf_Comment.csv.backward \
	# --relationships_backward:COMMENT_IS_LOCATED_IN ${basedir}/sf${SF}/${intermdir}/dynamic/Comment_isLocatedIn_Place.csv.backward \
	# --relationships_backward:HAS_TAG ${basedir}/sf${SF}/${intermdir}/dynamic/Comment_hasTag_Tag.csv.backward \
	# --relationships_backward:CONTAINER_OF ${basedir}/sf${SF}/${intermdir}/dynamic/Forum_containerOf_Post.csv.backward \
	# --relationships_backward:HAS_MODERATOR ${basedir}/sf${SF}/${intermdir}/dynamic/Forum_hasModerator_Person.csv.backward \
	# --relationships_backward:HAS_MEMBER ${basedir}/sf${SF}/${intermdir}/dynamic/Forum_hasMember_Person.csv.backward \
	# --relationships_backward:FORUM_HAS_TAG ${basedir}/sf${SF}/${intermdir}/dynamic/Forum_hasTag_Tag.csv.backward \
	# --relationships_backward:POST_HAS_TAG ${basedir}/sf${SF}/${intermdir}/dynamic/Post_hasTag_Tag.csv.backward \
	# --relationships_backward:POST_IS_LOCATED_IN ${basedir}/sf${SF}/${intermdir}/dynamic/Post_isLocatedIn_Place.csv.backward \
	# --relationships_backward:WORK_AT ${basedir}/sf${SF}/${intermdir}/dynamic/Person_workAt_Organisation.csv.backward \
	# --relationships_backward:ORG_IS_LOCATED_IN ${basedir}/sf${SF}/${intermdir}/static/Organisation_isLocatedIn_Place.csv.backward \
	# --relationships_backward:IS_PART_OF ${basedir}/sf${SF}/${intermdir}/static/Place_isPartOf_Place.csv.backward \
	# --relationships_backward:IS_SUBCLASS_OF ${basedir}/sf${SF}/${intermdir}/static/TagClass_isSubclassOf_TagClass.csv.backward \
	# --relationships_backward:HAS_TYPE ${basedir}/sf${SF}/${intermdir}/static/Tag_hasType_TagClass.csv.backward	
