#!/bin/bash

workload_type=$1
workspace=$2
debug_plan=$3

if [ "$#" -eq 1 ]; then
	workspace="/data/ldbc/sf1_test/"
	debug_plan_option=""
elif [ "$#" -eq 2 ]; then
	debug_plan_option=""
elif [ "$#" -eq 3 ]; then
	if [ $debug_plan -eq 1 ]; then
		debug_plan_option="--debug-planner"
	else
		debug_plan_option=""
	fi
fi

iterations="--num-iterations:5"

#./tbgpp-client/TurboGraph-S62 --workspace:/data/ldbc/sf1_test/ --query:"MATCH (a:Person)-[r1:KNOWS]->(b:Person)-[r2:KNOWS]->(c:Person) RETURN a.lastName, b.lastName, c.lastName" --debug-planner --index-join-only
#./tbgpp-client/TurboGraph-S62 --workspace:/data/ldbc/sf1_test/ --query:"MATCH (p:Person)<-[r:POST_HAS_CREATOR]-(message:Post) RETURN message.id" --debug-planner --index-join-only
#./tbgpp-client/TurboGraph-S62 --workspace:/data/ldbc/sf1_test/ --debug-planner --index-join-only
#./tbgpp-client/TurboGraph-S62 --workspace:/data/ldbc/sf1_test/ --index-join-only
#./tbgpp-client/TurboGraph-S62 --workspace:/data/ldbc/sf1_test/ --query:"MATCH (m:Comment)-[roc:REPLY_OF_COMMENT*0..8]->(n:Comment)-[ro:REPLY_OF]->(p:Post)<-[co:CONTAINER_OF]-(f:Forum) RETURN m.id, n.id, p.id, f.id" --debug-planner --index-join-only
#./tbgpp-client/TurboGraph-S62 --workspace:/data/ldbc/sf1_test/ --query:"MATCH (m:Comment)-[roc:REPLY_OF_COMMENT]->(n:Comment)-[ro:REPLY_OF]->(p:Post)<-[co:CONTAINER_OF]-(f:Forum) RETURN m.id, n.id, p.id, f.id" --debug-planner --index-join-only
#./tbgpp-client/TurboGraph-S62 --workspace:/data/ldbc/sf1_test/ --query:"MATCH (m:Comment)-[roc:REPLY_OF_COMMENT*0..8]->(n:Comment)-[ro:REPLY_OF]->(p:Post)-[co:HAS_CREATOR]->(f:Person) RETURN m.id, n.id, p.id, f.id" --debug-planner --index-join-only
#./tbgpp-client/TurboGraph-S62 --workspace:/data/ldbc/sf1_test/ --query:"MATCH (m:Comment)-[roc:REPLY_OF_COMMENT*0..8]->(n:Comment)-[ro:REPLY_OF]->(p:Post) RETURN m.id, n.id, p.id" --debug-planner --index-join-only
#./tbgpp-client/TurboGraph-S62 --workspace:/data/ldbc/sf1_test/ --query:"MATCH (m:Comment)-[roc:REPLY_OF_COMMENT*0..8]->(n:Comment)-[ro:REPLY_OF]->(p:Post)<-[co:CONTAINER_OF]-(f:Forum)-[hm:HAS_MODERATOR]->(mod:Person) RETURN f.id, f.title, mod.id, mod.firstName, mod.lastName" --index-join-only

run_query() {
	query_str=$1
	echo $query_str
	./build-debug/tbgpp-client/TurboGraph-S62 --workspace:${workspace} --query:"$query_str" ${debug_plan_option} --index-join-only ${iterations}
}

run_ldbc_s() {
	# LDBC Short
	echo "RUN LDBC Short"

	# LDBC IS1
	run_query "MATCH (n:Person {id: 65})-[r:IS_LOCATED_IN]->(p:Place) RETURN n.firstName, n.lastName, n.birthday, n.locationIP, n.browserUsed, p.id, n.gender, n.creationDate"
	# LDBC IS2

	# LDBC IS3
	run_query "MATCH (n:Person {id: 94})-[r:KNOWS]->(friend:Person) RETURN friend.id, friend.firstName, friend.lastName, r.creationDate ORDER BY r.creationDate DESC, friend.id ASC"
	# LDBC IS4
	run_query "MATCH (m:Comment {id: 557}) RETURN m.creationDate, m.content"
	# LDBC IS5
#	run_query "MATCH (m:Comment)-[r:HAS_CREATOR]->(p:Person) WHERE m.id = 557 RETURN p.id, p.firstName, p.lastName"
	run_query "MATCH (m:Comment {id: 557})-[r:HAS_CREATOR]->(p:Person) RETURN p.id, p.firstName, p.lastName"
	# LDBC IS6
	run_query "MATCH (m:Comment {id: 1099511628400})-[roc:REPLY_OF_COMMENT*0..8]->(n:Comment)-[ro:REPLY_OF]->(p:Post)<-[co:CONTAINER_OF]-(f:Forum)-[hm:HAS_MODERATOR]->(mod:Person) RETURN f.id, f.title, mod.id, mod.firstName, mod.lastName"
	# LDBC IS7
	run_query "MATCH (m:Post {id: 556})<-[:REPLY_OF]-(c:Comment)-[:HAS_CREATOR]->(p:Person) OPTIONAL MATCH (m)-[:POST_HAS_CREATOR]->(a:Person)-[r:KNOWS]-(p) RETURN c.id, c.content, c.creationDate, p.id, p.firstName, p.lastName"

}

run_ldbc_c() {
	# LDBC Complex
	echo "RUN LDBC Complex"

	# LDBC IS1
	#./tbgpp-client/TurboGraph-S62 --workspace:/data/ldbc/sf1_test/ --query:"" --index-join-only
}

run_ldbc() {
	run_ldbc_s
	run_ldbc_c
}

if [ $workload_type == "ldbc" ]; then
	run_ldbc
elif [ $workload_type == "ldbc_s" ]; then
	run_ldbc_s
elif [ $workload_type == "ldbc_c" ]; then
	run_ldbc_c
fi
