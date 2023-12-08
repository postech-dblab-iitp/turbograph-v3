#!/bin/bash
SF=1
basedir="/source-data/ldbc-hadoop/"
intermdir="/converted/"
num_total_threads=4
output_dir="/data"
debug=""
if [ $# -gt 0 ]; then
    debug="-gdb"
fi

rm -rf $output_dir/*

export TCP_BASE_PORTNUM=40000

cd /turbograph-v3/build
./tbgpp-graph-store/store &
pid_store=$!
echo -n "Store process: "
echo $pid_store

cd /turbograph-v3/build
for (( i=0; i<$num_total_threads; i++ )) do 
	output_dir_for_this_thread=$output_dir"/"$i
	mkdir $output_dir_for_this_thread
	./tbgpp-graph-store/catalog_test_catalog_server $output_dir_for_this_thread &
	pid_cat_server=$!
	echo -n "generated catalog server "
	echo -n $i
	echo -n ": "
	echo $pid_cat_server
	echo -n "for "
	echo -n $output_dir_for_this_thread
	pid_cat_server_arr+=($pid_cat_server)
done

terminate() {
	kill $pid_store
	for cat_server_pid in "${pid_cat_server_arr[@]}"; do	
		kill $cat_server_pid
	done
	exit 0
}

trap terminate SIGINT


mpirun -n ${num_total_threads} ${debug} /turbograph-v3/build/tbgpp-graph-store/graph_partition_simulation_new_new \
	--output_dir:"/data" \
	--nodes:Person ${basedir}/sf${SF}/${intermdir}/dynamic/Person.csv \
	--nodes:Forum ${basedir}/sf${SF}/${intermdir}/dynamic/Forum.csv \
	--nodes:Organisation ${basedir}/sf${SF}/${intermdir}/static/Organisation.csv \
	--nodes:Place ${basedir}/sf${SF}/${intermdir}/static/Place.csv \
	--nodes:Tag ${basedir}/sf${SF}/${intermdir}/static/Tag.csv \
	--nodes:TagClass ${basedir}/sf${SF}/${intermdir}/static/TagClass.csv \
	--nodes:Comment:Message ${basedir}/sf${SF}/${intermdir}/dynamic/Comment.csv \
	--nodes:Post:Message ${basedir}/sf${SF}/${intermdir}/dynamic/Post.csv
	# --relationships:HAS_CREATOR ${basedir}/sf${SF}/${intermdir}/dynamic/Comment_hasCreator_Person.csv \
	# --relationships_backward:HAS_CREATOR ${basedir}/sf${SF}/${intermdir}/dynamic/Comment_hasCreator_Person.csv.backward \
	# --relationships:POST_HAS_CREATOR ${basedir}/sf${SF}/${intermdir}/dynamic/Post_hasCreator_Person.csv \
	# --relationships_backward:POST_HAS_CREATOR ${basedir}/sf${SF}/${intermdir}/dynamic/Post_hasCreator_Person.csv.backward \
	# --relationships:IS_LOCATED_IN ${basedir}/sf${SF}/${intermdir}/dynamic/Person_isLocatedIn_Place.csv \
	# --relationships_backward:IS_LOCATED_IN ${basedir}/sf${SF}/${intermdir}/dynamic/Person_isLocatedIn_Place.csv.backward \
	# --relationships:KNOWS ${basedir}/sf${SF}/${intermdir}/dynamic/Person_knows_Person.csv \
	# --relationships_backward:KNOWS ${basedir}/sf${SF}/${intermdir}/dynamic/Person_knows_Person.csv.backward \
	# --relationships:LIKES ${basedir}/sf${SF}/${intermdir}/dynamic/Person_likes_Comment.csv \
	# --relationships_backward:LIKES ${basedir}/sf${SF}/${intermdir}/dynamic/Person_likes_Comment.csv.backward \
	# --relationships:LIKES_POST ${basedir}/sf${SF}/${intermdir}/dynamic/Person_likes_Post.csv \
	# --relationships_backward:LIKES_POST ${basedir}/sf${SF}/${intermdir}/dynamic/Person_likes_Post.csv.backward \
	# --relationships:HAS_INTEREST ${basedir}/sf${SF}/${intermdir}/dynamic/Person_hasInterest_Tag.csv \
	# --relationships_backward:HAS_INTEREST ${basedir}/sf${SF}/${intermdir}/dynamic/Person_hasInterest_Tag.csv.backward \
	# --relationships:STUDY_AT ${basedir}/sf${SF}/${intermdir}/dynamic/Person_studyAt_Organisation.csv \
	# --relationships_backward:STUDY_AT ${basedir}/sf${SF}/${intermdir}/dynamic/Person_studyAt_Organisation.csv.backward \
	# --relationships:REPLY_OF ${basedir}/sf${SF}/${intermdir}/dynamic/Comment_replyOf_Post.csv \
	# --relationships_backward:REPLY_OF ${basedir}/sf${SF}/${intermdir}/dynamic/Comment_replyOf_Post.csv.backward \
	# --relationships:REPLY_OF_COMMENT ${basedir}/sf${SF}/${intermdir}/dynamic/Comment_replyOf_Comment.csv \
	# --relationships_backward:REPLY_OF_COMMENT ${basedir}/sf${SF}/${intermdir}/dynamic/Comment_replyOf_Comment.csv.backward \
	# --relationships:COMMENT_IS_LOCATED_IN ${basedir}/sf${SF}/${intermdir}/dynamic/Comment_isLocatedIn_Place.csv \
	# --relationships_backward:COMMENT_IS_LOCATED_IN ${basedir}/sf${SF}/${intermdir}/dynamic/Comment_isLocatedIn_Place.csv.backward \
	# --relationships:HAS_TAG ${basedir}/sf${SF}/${intermdir}/dynamic/Comment_hasTag_Tag.csv \
	# --relationships_backward:HAS_TAG ${basedir}/sf${SF}/${intermdir}/dynamic/Comment_hasTag_Tag.csv.backward \
	# --relationships:CONTAINER_OF ${basedir}/sf${SF}/${intermdir}/dynamic/Forum_containerOf_Post.csv \
	# --relationships_backward:CONTAINER_OF ${basedir}/sf${SF}/${intermdir}/dynamic/Forum_containerOf_Post.csv.backward \
	# --relationships:HAS_MODERATOR ${basedir}/sf${SF}/${intermdir}/dynamic/Forum_hasModerator_Person.csv \
	# --relationships_backward:HAS_MODERATOR ${basedir}/sf${SF}/${intermdir}/dynamic/Forum_hasModerator_Person.csv.backward \
	# --relationships:HAS_MEMBER ${basedir}/sf${SF}/${intermdir}/dynamic/Forum_hasMember_Person.csv \
	# --relationships_backward:HAS_MEMBER ${basedir}/sf${SF}/${intermdir}/dynamic/Forum_hasMember_Person.csv.backward \
	# --relationships:FORUM_HAS_TAG ${basedir}/sf${SF}/${intermdir}/dynamic/Forum_hasTag_Tag.csv \
	# --relationships_backward:FORUM_HAS_TAG ${basedir}/sf${SF}/${intermdir}/dynamic/Forum_hasTag_Tag.csv.backward \
	# --relationships:POST_HAS_TAG ${basedir}/sf${SF}/${intermdir}/dynamic/Post_hasTag_Tag.csv \
	# --relationships_backward:POST_HAS_TAG ${basedir}/sf${SF}/${intermdir}/dynamic/Post_hasTag_Tag.csv.backward \
	# --relationships:POST_IS_LOCATED_IN ${basedir}/sf${SF}/${intermdir}/dynamic/Post_isLocatedIn_Place.csv \
	# --relationships_backward:POST_IS_LOCATED_IN ${basedir}/sf${SF}/${intermdir}/dynamic/Post_isLocatedIn_Place.csv.backward \
	# --relationships:WORK_AT ${basedir}/sf${SF}/${intermdir}/dynamic/Person_workAt_Organisation.csv \
	# --relationships_backward:WORK_AT ${basedir}/sf${SF}/${intermdir}/dynamic/Person_workAt_Organisation.csv.backward \
	# --relationships:ORG_IS_LOCATED_IN ${basedir}/sf${SF}/${intermdir}/static/Organisation_isLocatedIn_Place.csv \
	# --relationships_backward:ORG_IS_LOCATED_IN ${basedir}/sf${SF}/${intermdir}/static/Organisation_isLocatedIn_Place.csv.backward \
	# --relationships:IS_PART_OF ${basedir}/sf${SF}/${intermdir}/static/Place_isPartOf_Place.csv \
	# --relationships_backward:IS_PART_OF ${basedir}/sf${SF}/${intermdir}/static/Place_isPartOf_Place.csv.backward \
	# --relationships:IS_SUBCLASS_OF ${basedir}/sf${SF}/${intermdir}/static/TagClass_isSubclassOf_TagClass.csv \
	# --relationships_backward:IS_SUBCLASS_OF ${basedir}/sf${SF}/${intermdir}/static/TagClass_isSubclassOf_TagClass.csv.backward \
	# --relationships:HAS_TYPE ${basedir}/sf${SF}/${intermdir}/static/Tag_hasType_TagClass.csv \
	# --relationships_backward:HAS_TYPE ${basedir}/sf${SF}/${intermdir}/static/Tag_hasType_TagClass.csv.backward	

terminate

# NOTE: If cmake cannot find MPI, try setting followings: 
# export I_MPI_ROOT=/path/to/intel/mpi
# export PATH=/path/to/intel/mpi/bin:$PATH
# export LD_LIBRARY_PATH=/path/to/intel/mpi/lib:$LD_LIBRARY_PATH
# And remove CMakeCache.txt too.
# Also, set export TCP_BASE_PORTNUM=40000 /here 40000 is arbitrary number..