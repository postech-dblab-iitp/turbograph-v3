#!/bin/bash

# Define the possible values for each configuration
BUILD_DIR="/turbograph-v3/build-release/tools/"
scale_factors=("1")
source_dir_base="/source-data/ldbc/"
target_dir_base="/data/ldbc/"

# Loop over all combinations of cluster algorithms, cost models, and layering orders
for scale_factor in "${scale_factors[@]}"; do
    data_dir="${source_dir_base}/sf${scale_factor}"
    target_dir="${target_dir_base}/sf${scale_factor}"
    
    rm -rf ${target_dir}
    mkdir -p ${target_dir}
    
    ${BUILD_DIR}/store 365GB &
    ${BUILD_DIR}/catalog_server ${target_dir} &
    sleep 10

    ${BUILD_DIR}/bulkload \
        --log-level trace \
        --output_dir ${target_dir} \
        --nodes Person ${data_dir}/dynamic/Person.csv \
        # --nodes Comment:Message ${data_dir}/dynamic/Comment.csv \
        # --nodes Post:Message ${data_dir}/dynamic/Post.csv \
        # --nodes Forum ${data_dir}/dynamic/Forum.csv \
        # --nodes Organisation ${data_dir}/static/Organisation.csv \
        # --nodes Place ${data_dir}/static/Place.csv \
        # --nodes Tag ${data_dir}/static/Tag.csv \
        # --nodes TagClass ${data_dir}/static/TagClass.csv \
        # --relationships HAS_CREATOR ${data_dir}/dynamic/Comment_hasCreator_Person.csv \
        # --relationships_backward HAS_CREATOR ${data_dir}/dynamic/Comment_hasCreator_Person.csv.backward \
        # --relationships POST_HAS_CREATOR ${data_dir}/dynamic/Post_hasCreator_Person.csv \
        # --relationships_backward POST_HAS_CREATOR ${data_dir}/dynamic/Post_hasCreator_Person.csv.backward \
        # --relationships IS_LOCATED_IN ${data_dir}/dynamic/Person_isLocatedIn_Place.csv \
        # --relationships_backward IS_LOCATED_IN ${data_dir}/dynamic/Person_isLocatedIn_Place.csv.backward \
        # --relationships KNOWS ${data_dir}/dynamic/Person_knows_Person.csv \
        # --relationships_backward KNOWS ${data_dir}/dynamic/Person_knows_Person.csv.backward \
        # --relationships LIKES ${data_dir}/dynamic/Person_likes_Comment.csv \
        # --relationships_backward LIKES ${data_dir}/dynamic/Person_likes_Comment.csv.backward \
        # --relationships LIKES_POST ${data_dir}/dynamic/Person_likes_Post.csv \
        # --relationships_backward LIKES_POST ${data_dir}/dynamic/Person_likes_Post.csv.backward \
        # --relationships HAS_INTEREST ${data_dir}/dynamic/Person_hasInterest_Tag.csv \
        # --relationships_backward HAS_INTEREST ${data_dir}/dynamic/Person_hasInterest_Tag.csv.backward \
        # --relationships STUDY_AT ${data_dir}/dynamic/Person_studyAt_Organisation.csv \
        # --relationships_backward STUDY_AT ${data_dir}/dynamic/Person_studyAt_Organisation.csv.backward \
        # --relationships REPLY_OF ${data_dir}/dynamic/Comment_replyOf_Post.csv \
        # --relationships_backward REPLY_OF ${data_dir}/dynamic/Comment_replyOf_Post.csv.backward \
        # --relationships REPLY_OF_COMMENT ${data_dir}/dynamic/Comment_replyOf_Comment.csv \
        # --relationships_backward REPLY_OF_COMMENT ${data_dir}/dynamic/Comment_replyOf_Comment.csv.backward \
        # --relationships COMMENT_IS_LOCATED_IN ${data_dir}/dynamic/Comment_isLocatedIn_Place.csv \
        # --relationships_backward COMMENT_IS_LOCATED_IN ${data_dir}/dynamic/Comment_isLocatedIn_Place.csv.backward \
        # --relationships HAS_TAG ${data_dir}/dynamic/Comment_hasTag_Tag.csv \
        # --relationships_backward HAS_TAG ${data_dir}/dynamic/Comment_hasTag_Tag.csv.backward \
        # --relationships CONTAINER_OF ${data_dir}/dynamic/Forum_containerOf_Post.csv \
        # --relationships_backward CONTAINER_OF ${data_dir}/dynamic/Forum_containerOf_Post.csv.backward \
        # --relationships HAS_MODERATOR ${data_dir}/dynamic/Forum_hasModerator_Person.csv \
        # --relationships_backward HAS_MODERATOR ${data_dir}/dynamic/Forum_hasModerator_Person.csv.backward \
        # --relationships HAS_MEMBER ${data_dir}/dynamic/Forum_hasMember_Person.csv \
        # --relationships_backward HAS_MEMBER ${data_dir}/dynamic/Forum_hasMember_Person.csv.backward \
        # --relationships FORUM_HAS_TAG ${data_dir}/dynamic/Forum_hasTag_Tag.csv \
        # --relationships_backward FORUM_HAS_TAG ${data_dir}/dynamic/Forum_hasTag_Tag.csv.backward \
        # --relationships POST_HAS_TAG ${data_dir}/dynamic/Post_hasTag_Tag.csv \
        # --relationships_backward POST_HAS_TAG ${data_dir}/dynamic/Post_hasTag_Tag.csv.backward \
        # --relationships POST_IS_LOCATED_IN ${data_dir}/dynamic/Post_isLocatedIn_Place.csv \
        # --relationships_backward POST_IS_LOCATED_IN ${data_dir}/dynamic/Post_isLocatedIn_Place.csv.backward \
        # --relationships WORK_AT ${data_dir}/dynamic/Person_workAt_Organisation.csv \
        # --relationships_backward WORK_AT ${data_dir}/dynamic/Person_workAt_Organisation.csv.backward \
        # --relationships ORG_IS_LOCATED_IN ${data_dir}/static/Organisation_isLocatedIn_Place.csv \
        # --relationships_backward ORG_IS_LOCATED_IN ${data_dir}/static/Organisation_isLocatedIn_Place.csv.backward \
        # --relationships IS_PART_OF ${data_dir}/static/Place_isPartOf_Place.csv \
        # --relationships_backward IS_PART_OF ${data_dir}/static/Place_isPartOf_Place.csv.backward \
        # --relationships IS_SUBCLASS_OF ${data_dir}/static/TagClass_isSubclassOf_TagClass.csv \
        # --relationships_backward IS_SUBCLASS_OF ${data_dir}/static/TagClass_isSubclassOf_TagClass.csv.backward \
        # --relationships HAS_TYPE ${data_dir}/static/Tag_hasType_TagClass.csv \
        # --relationships_backward HAS_TYPE ${data_dir}/static/Tag_hasType_TagClass.csv.backward

    pkill -f store
    pkill -f catalog_server
    sleep 5
done
