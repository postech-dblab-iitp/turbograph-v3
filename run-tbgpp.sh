#!/bin/bash

workload_type=$1
workspace=$2
debug_plan=$3

if [ "$#" -eq 1 ]; then
	workspace="/data/tpch/sf1_swizzling2/"
	debug_plan_option=""
elif [ "$#" -eq 2 ]; then
	debug_plan_option=""
elif [ "$#" -eq 3 ]; then
	if [ $debug_plan -eq 1 ]; then
		debug_plan_option="--debug-orca"
	else
		debug_plan_option=""
	fi
fi

iterations="--num-iterations:1"

run_query() {
	query_str=$1
	if [ "$#" -eq 2 ]; then
		skip_query=$2
		if [ $skip_query -eq 1 ]; then
			return 0
		fi
	fi

	echo "$query_str"
	./build_release/tbgpp-client/TurboGraph-S62 --workspace:${workspace} --query:"$query_str" ${debug_plan_option} --index-join-only ${iterations} --join-order-optimizer:query --explain --debug-orca
	# ./build_release/tbgpp-client/TurboGraph-S62 --workspace:${workspace} --query:"$query_str" ${debug_plan_option} --index-join-only ${iterations} --join-order-optimizer:greedy --profile --explain
	# ./build_release/tbgpp-client/TurboGraph-S62 --workspace:${workspace} --query:"$query_str" ${debug_plan_option} --index-join-only ${iterations} --join-order-optimizer:exhaustive --profile --explain
	# ./build_release/tbgpp-client/TurboGraph-S62 --workspace:${workspace} --query:"$query_str" ${debug_plan_option} --index-join-only ${iterations} --join-order-optimizer:exhaustive2 --profile --explain
}

run_ldbc_s() {
	# LDBC Short
	echo "RUN LDBC Short"

	# LDBC IS1 Profile of a person
	# Place -> City
	run_query "MATCH (n:Person {id: 65})-[r:IS_LOCATED_IN]->(p:Place)
		   RETURN
		   	n.firstName AS firstName,
			n.lastName AS lastName,
			n.birthday AS birthday,
			n.locationIP AS locationIP,
			n.browserUsed AS browserUsed,
			p.id AS cityId,
			n.gender AS gender,
			n.creationDate AS creationDate"

	# LDBC IS2 Recent messages of a person
	run_query "MATCH (:Person {id: 94})<-[:HAS_CREATOR]-(message:Comment)
			   WITH
				message,
				message.id AS messageId,
				message.creationDate AS messageCreationDate
			   ORDER BY messageCreationDate DESC, messageId ASC
			   LIMIT 10
			   MATCH (message)-[:REPLY_OF_COMMENT*0..8]->(n:Comment)-[ro:REPLY_OF]->(post:Post),
					(post)-[:POST_HAS_CREATOR]->(person:Person)
			   RETURN
				messageId,
				message.content AS messageContent,
				messageCreationDate,
				post.id AS postId,
				person.id AS personId,
				person.firstName AS personFirstName,
				person.lastName AS personLastName
			   ORDER BY messageCreationDate DESC, messageId ASC"
	
	# LDBC IS3 Friends of a Person
	run_query "MATCH (n:Person {id: 94})-[r:KNOWS]->(friend:Person)
		RETURN
			friend.id AS personId,
			friend.firstName AS firstName,
			friend.lastName AS lastName,
			r.creationDate AS friendshipCreationDate
		ORDER BY
			friendshipCreationDate DESC,
			personId ASC"

	# LDBC IS4 Content of a message
	run_query "MATCH (m:Comment {id: 557})
		RETURN
			m.creationDate as messageCreationDate,
			m.content as messageContent"
	
	# LDBC IS5 Creator of a message
	run_query "MATCH (m:Comment {id: 557})-[r:HAS_CREATOR]->(p:Person)
		RETURN
			p.id AS personId,
			p.firstName AS firstName,
			p.lastName AS lastName"

	# LDBC IS6 Forum of a message
	run_query "MATCH
			(m:Comment {id: 1099511628400})-[roc:REPLY_OF_COMMENT*0..8]->(n:Comment)-[ro:REPLY_OF]->(p:Post)
			<-[co:CONTAINER_OF]-(f:Forum)-[hm:HAS_MODERATOR]->(mod:Person)
		RETURN
			f.id AS forumId,
			f.title AS forumTitle,
			mod.id AS moderatorId,
			mod.firstName AS moderatorFirstName,
			mod.lastName AS moderatorLastName;"

	# LDBC IS7 Replies of a message
	run_query "MATCH (m:Post {id: 556 })<-[:REPLY_OF]-(c:Comment)-[:HAS_CREATOR]->(p:Person)
	    OPTIONAL MATCH (m)-[:POST_HAS_CREATOR]->(a:Person)<-[r:KNOWS]-(p)
	    RETURN c.id AS commentId,
			c.content AS commentContent,
			c.creationDate AS commentCreationDate,
			p.id AS replyAuthorId,
			p.firstName AS replyAuthorFirstName,
			p.lastName AS replyAuthorLastName,
		CASE r._id
		    WHEN null THEN false
		    ELSE true
		END AS replyAuthorKnowsOriginalMessageAuthor
	    ORDER BY commentCreationDate DESC, replyAuthorId"

}

run_ldbc_c1() {
	# LDBC IC1 Transitive friends with certain name
	run_query "MATCH (p:Person {id: 94}), (friend:Person {firstName: 'Jose'})
		WHERE NOT p=friend
		WITH p, friend
		MATCH path = shortestPath((p)-[:KNOWS*1..3]-(friend))
		WITH min(length(path)) AS distance, friend
		ORDER BY
			distance ASC,
			friend.lastName ASC,
			toInteger(friend.id) ASC
		LIMIT 20

		MATCH (friend)-[:IS_LOCATED_IN]->(friendCity:City)
		OPTIONAL MATCH (friend)-[studyAt:STUDY_AT]->(uni:University)-[:IS_LOCATED_IN]->(uniCity:City)
		WITH friend, collect(
			CASE uni.name
				WHEN null THEN null
				ELSE [uni.name, studyAt.classYear, uniCity.name]
			END ) AS unis, friendCity, distance

		OPTIONAL MATCH (friend)-[workAt:WORK_AT]->(company:Company)-[:IS_LOCATED_IN]->(companyCountry:Country)
		WITH friend, collect(
			CASE company.name
				WHEN null THEN null
				ELSE [company.name, workAt.workFrom, companyCountry.name]
			END ) AS companies, unis, friendCity, distance

		RETURN
			friend.id AS friendId,
			friend.lastName AS friendLastName,
			distance AS distanceFromPerson,
			friend.birthday AS friendBirthday,
			friend.creationDate AS friendCreationDate,
			friend.gender AS friendGender,
			friend.browserUsed AS friendBrowserUsed,
			friend.locationIP AS friendLocationIp,
			friend.email AS friendEmails,
			friend.speaks AS friendLanguages,
			friendCity.name AS friendCityName,
			unis AS friendUniversities,
			companies AS friendCompanies
		ORDER BY
			distanceFromPerson ASC,
			friendLastName ASC,
			toInteger(friendId) ASC
		LIMIT 20" 1

}

run_ldbc_c2() {
	# LDBC IC2 Recent messages by your friends
	run_query "MATCH (:Person {id: 94 })-[:KNOWS]->(friend:Person)<-[:HAS_CREATOR]-(message:Comment)
		WHERE message.creationDate <= 1287230400000
		RETURN
			friend.id AS personId,
			friend.firstName AS personFirstName,
			friend.lastName AS personLastName,
			message.id AS postOrCommentId,
			message.content AS postOrCommentContent,
			message.creationDate AS postOrCommentCreationDate
		ORDER BY
			postOrCommentCreationDate DESC,
			postOrCommentId ASC
		LIMIT 20" 0
}

run_ldbc_c3() {
	# LDBC IC3 Friends and friends of friends that have been to given countries
	run_query "MATCH (countryX:Country {name: 'Angola' }),
		(countryY:Country {name: 'Colombia' }),
		(person:Person {id: 6597069766734 })
		WITH person, countryX, countryY
		LIMIT 1
		MATCH (city:City)-[:IS_PART_OF]->(country:Country)
		WHERE country IN [countryX, countryY]
		WITH person, countryX, countryY, collect(city) AS cities
		MATCH (person)-[:KNOWS*1..2]-(friend)-[:IS_LOCATED_IN]->(city)
		WHERE NOT person=friend AND NOT city IN cities
		WITH DISTINCT friend, countryX, countryY
		MATCH (friend)<-[:HAS_CREATOR]-(message),
			(message)-[:IS_LOCATED_IN]->(country)
		WHERE 1277812800000 > message.creationDate >= 1275393600000 AND
			country IN [countryX, countryY]
		WITH friend,
			CASE WHEN country=countryX THEN 1 ELSE 0 END AS messageX,
			CASE WHEN country=countryY THEN 1 ELSE 0 END AS messageY
		WITH friend, sum(messageX) AS xCount, sum(messageY) AS yCount
		WHERE xCount>0 AND yCount>0
		RETURN friend.id AS friendId,
			friend.firstName AS friendFirstName,
			friend.lastName AS friendLastName,
			xCount,
			yCount,
			xCount + yCount AS xyCount
		ORDER BY xyCount DESC, friendId ASC
		LIMIT 20" 1
	run_query "MATCH (countryX:Place {name: 'Angola' }),
		(countryY:Place {name: 'Colombia' }),
		(person:Person {id: 94 })
		WITH person, countryX, countryY
		LIMIT 1
		MATCH (person)-[:KNOWS*1..2]->(friend:Person)-[:IS_LOCATED_IN]->(city:Place)
		RETURN person.id, countryX.name, countryY.name, city.name" 0
	#run_query "MATCH (countryX:Place {name: 'Germany' }),
	#      (countryY:Place {name: 'Hungary' }),
	run_query "MATCH (countryX:Place {name: 'Angola' }),
	      (countryY:Place {name: 'Colombia' }),
	      (person:Person {id: 94 })
	WITH person, countryX, countryY
	LIMIT 1
	MATCH (person)-[:KNOWS*1..2]-(friend:Person)-[:IS_LOCATED_IN]->(city:Place)
	WHERE
	  NOT person=friend
	  AND NOT EXISTS {
	    MATCH (city)-[:IS_PART_OF]->(country:Place)
	    WHERE country = countryX OR country = countryY
	  }
	RETURN person;" 1
}

run_ldbc_c4() {
	# LDBC IC4 New topics
	# run_query "MATCH (person:Person {id: 4398046511333 })-[:KNOWS]-(friend:Person),
    #   	(friend)<-[:POST_HAS_CREATOR]-(post:Post)-[:POST_HAS_TAG]->(tag)
	# 	WITH DISTINCT tag, post
	# 	WITH tag,
	# 		CASE
	# 		WHEN 1277856000000 > post.creationDate >= 1275350400000 THEN 1
	# 		ELSE 0
	# 		END AS valid,
	# 		CASE
	# 		WHEN 1275350400000 > post.creationDate THEN 1
	# 		ELSE 0
	# 		END AS inValid
	# 	WITH tag, sum(valid) AS postCount, sum(inValid) AS inValidPostCount
	# 	WHERE postCount>0 AND inValidPostCount=0
	# 	RETURN tag.name AS tagName, postCount
	# 	ORDER BY postCount DESC, tagName ASC
	# 	LIMIT 10" 1

	# no > >= in comparison - kuzu parser limitation
	# with tag, ... -> kuzu does not convert to tag.id, tag.property.... , maybe kuzu bug
	run_query "MATCH (person:Person {id: 94 })-[:KNOWS]-(friend:Person),
      	(friend)<-[:POST_HAS_CREATOR]-(post:Post)-[:POST_HAS_TAG]->(tag:Tag)
		WITH DISTINCT tag, post
		WITH tag,
			CASE
			WHEN post.creationDate >= 1275350400000 THEN 1
			ELSE 0
			END AS valid,
			CASE
			WHEN post.creationDate < 1275350400000 THEN 1
			ELSE 0
			END AS inValid
		WITH tag.id AS tagid, tag.name AS tagName, sum(valid) AS postCount, sum(inValid) AS inValidPostCount
		WHERE postCount>0
		RETURN tagName
		ORDER BY tagName ASC
		LIMIT 10" 0
}

run_ldbc_c5() {
	# LDBC IC5 New groups
		# WHERE NOT person=friend
		# collect(friend) AS friends
		# WHERE membership.joinDate > 1288612800000
	run_query "MATCH (person:Person { id: 6597069766734 })-[:KNOWS*1..2]-(friend:Person)
		WITH DISTINCT friend
		MATCH (friend)<-[membership:HAS_MEMBER]-(forum:Forum)
		WHERE
			membership.joinDate > 1288612800000
		WITH
			forum,
			friend
		MATCH (friend)<-[:POST_HAS_CREATOR]-(post:Post)<-[:CONTAINER_OF]-(forum)
		WITH
			forum,
			count(post) AS postCount
		RETURN
			forum.title AS forumName,
			postCount
		ORDER BY
			postCount DESC,
			forum.id ASC
		LIMIT 20" 1
	run_query "MATCH (person:Person { id: 94 })-[:KNOWS*1..2]->(friend:Person)
		WITH DISTINCT friend
		MATCH (friend)<-[membership:HAS_MEMBER]-(forum:Forum)
		WITH
			forum,
			friend
		MATCH (post:Post)<-[:CONTAINER_OF]-(forum)
		WITH
			forum, post
		RETURN
			forum.title AS forumName,
			count(post.id) AS postCount
		LIMIT 20" 0
}

run_ldbc_c6() {
	# LDBC IC6 Tag co-occurrence
	# run_query "MATCH (knownTag:Tag { name: \"Carl_Gustaf_Emil_Mannerheim\" })
	# 	WITH knownTag.id as knownTagId

	# 	MATCH (person:Person { id: 4398046511333 })-[:KNOWS*1..2]-(friend)
	# 	WHERE NOT person=friend
	# 	WITH
	# 		knownTagId,
	# 		collect(distinct friend) as friends
	# 	UNWIND friends as f
	# 		MATCH (f)<-[:HAS_CREATOR]-(post:Post),
	# 			(post)-[:HAS_TAG]->(t:Tag{id: knownTagId}),
	# 			(post)-[:HAS_TAG]->(tag:Tag)
	# 		WHERE NOT t = tag
	# 		WITH
	# 			tag.name as tagName,
	# 			count(post) as postCount
	# 	RETURN
	# 		tagName,
	# 		postCount
	# 	ORDER BY
	# 		postCount DESC,
	# 		tagName ASC
	# 	LIMIT 10" 1

	run_query "MATCH (person:Person {id : 94 })-[:KNOWS*1..2]->(friend:Person)
		WHERE NOT person = friend
		WITH friend
		MATCH (knownTag:Tag { name: 'Carl_Gustaf_Emil_Mannerheim' })
		WITH friend, knownTag.id as knownTagId
		MATCH (friend)<-[:POST_HAS_CREATOR]-(post:Post),
		      (post)-[:POST_HAS_TAG]->(t:Tag {id: knownTagId}),
		      (post)-[:POST_HAS_TAG]->(tag:Tag)
		WITH tag.name as tagName, count(post) as postCount
		RETURN 
			tagName, 
			postCount
		ORDER BY
			postCount DESC,
			tagName ASC
		LIMIT 10" 0
}

run_ldbc_c7() {
	# LDBC IC7 Recent likers
	run_query "MATCH (person:Person {id: 4398046511268})<-[:HAS_CREATOR]-(message:Message)<-[like:LIKES]-(liker:Person)
		WITH liker, message, like.creationDate AS likeTime, person
		ORDER BY likeTime DESC, toInteger(message.id) ASC
		WITH liker, head(collect({msg: message, likeTime: likeTime})) AS latestLike, person
		RETURN
			liker.id AS personId,
			liker.firstName AS personFirstName,
			liker.lastName AS personLastName,
			latestLike.likeTime AS likeCreationDate,
			latestLike.msg.id AS commentOrPostId,
			coalesce(latestLike.msg.content, latestLike.msg.imageFile) AS commentOrPostContent,
			toInteger(round(toFloat(latestLike.likeTime - latestLike.msg.creationDate)/1000.0)/60.0) AS minutesLatency,
			not((liker)-[:KNOWS]-(person)) AS isNew
		ORDER BY
			likeCreationDate DESC,
			toInteger(personId) ASC
		LIMIT 20" 1
	run_query "MATCH (person:Person {id: 94})<-[:HAS_CREATOR]-(message:Comment)<-[like:LIKES]-(liker:Person)
		WITH liker, message, like, person
		ORDER BY like.creationDate DESC, message.id ASC
		RETURN
			liker.id AS personId,
			liker.firstName AS personFirstName,
			liker.lastName AS personLastName,
			first(message.id) AS msg_id,
			first(like.creationDate) AS likeCreationDate,
			person._id AS person_id
		ORDER BY
			likeCreationDate DESC,
			personId ASC
		LIMIT 20" 0
}

run_ldbc_c8() {
	# LDBC IC8 Recent replies
	run_query "MATCH (start:Person {id: 94})<-[:POST_HAS_CREATOR]-(p:Post)<-[:REPLY_OF]-(comment:Comment)-[:HAS_CREATOR]->(person:Person)
		RETURN
			person.id AS personId,
			person.firstName AS personFirstName,
			person.lastName AS personLastName,
			comment.creationDate AS commentCreationDate,
			comment.id AS commentId,
			comment.content AS commentContent
		ORDER BY
			commentCreationDate DESC,
			commentId ASC
		LIMIT 20" 0
}

run_ldbc_c9() {
	# LDBC IC9 Recent messages by friends or friends of friends
	# run_query "MATCH (root:Person {id: 4398046511268 })-[:KNOWS*1..2]-(friend:Person)
	# 	WHERE NOT friend = root
	# 	WITH collect(distinct friend) as friends
	# 	UNWIND friends as friend
	# 		MATCH (friend)<-[:HAS_CREATOR]-(message:Message)
	# 		WHERE message.creationDate < 1289908800000
	# 	RETURN
	# 		friend.id AS personId,
	# 		friend.firstName AS personFirstName,
	# 		friend.lastName AS personLastName,
	# 		message.id AS commentOrPostId,
	# 		coalesce(message.content,message.imageFile) AS commentOrPostContent,
	# 		message.creationDate AS commentOrPostCreationDate
	# 	ORDER BY
	# 		commentOrPostCreationDate DESC,
	# 		message.id ASC
	# 	LIMIT 20" 1

	run_query "MATCH (root:Person {id: 4398046511268 })-[:KNOWS*1..2]->(friend:Person)
		WITH DISTINCT friend
		MATCH (friend)<-[:HAS_CREATOR]-(message:Comment)
		WHERE message.creationDate < 1289908800000
		RETURN
			friend.id AS personId,
			friend.firstName AS personFirstName,
			friend.lastName AS personLastName,
			message.id AS commentOrPostId,
			message.content AS commentOrPostContent,
			message.creationDate AS commentOrPostCreationDate
		ORDER BY
			commentOrPostCreationDate DESC,
			commentOrPostId ASC
		LIMIT 20" 0
}

run_ldbc_c10() {
	# LDBC IC10 Friend recommendation
	run_query "MATCH (person:Person {id: 94})-[:KNOWS*2..2]-(friend),
       		(friend)-[:IS_LOCATED_IN]->(city:City)
	WHERE NOT friend=person AND
		NOT EXISTS { (friend)-[:KNOWS]-(person) }
	WITH person, city, friend, friend as birthday
	WHERE  (birthday.month=5 AND birthday.day>=21) OR
        	(birthday.month=6 AND birthday.day<22)
	WITH DISTINCT friend, city, person
	OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post:Post)
	WITH friend, city, person, post, (CASE WHEN EXISTS { MATCH (post)-[:HAS_TAG]->()<-[:HAS_INTEREST]-(person) } THEN 1 ELSE 0 END) AS postCommon
	WITH friend, city, person, count(post) AS postCount, sum(postCommon) AS commonPostCount
	RETURN friend.id AS personId,
		friend.firstName AS personFirstName,
		friend.lastName AS personLastName,
       		commonPostCount - (postCount - commonPostCount) AS commonInterestScore,
       		friend.gender AS personGender, 
       		city.name AS personCityName
	ORDER BY commonInterestScore DESC, personId ASCLIMIT 10" 1
	run_query "MATCH (person:Person {id: 96})-[:KNOWS*2..2]->(friend:Person),
		(friend)-[:IS_LOCATED_IN]->(city:Place)
	WHERE NOT friend=person
	WITH person, city, friend, friend.birthday as birthday
	WHERE (birthday >= 378086400000 AND birthday < 541209600000)
	WITH DISTINCT friend, city, person
	OPTIONAL MATCH (friend)<-[:POST_HAS_CREATOR]-(post:Post)
	WITH friend, city, person, count(post) AS postCount
	RETURN friend.id AS personId,
	       friend.firstName AS personFirstName,
	       friend.lastName AS personLastName,
	       postCount AS commonInterestScore,
	       friend.gender AS personGender,
	       city.name AS personCityName
	ORDER BY commonInterestScore DESC, personId ASC
	LIMIT 10" 0
}

run_ldbc_c11() {
	# LDBC IC11 Job referral
		#WHERE person._id <> friend._id
		#WHERE workAt.workFrom < 2011
	run_query "MATCH (person:Person {id: 94})-[:KNOWS*1..2]->(friend:Person)
		WITH DISTINCT friend
		MATCH (friend)-[workAt:WORK_AT]->(company:Organisation {label: 'Company'})-[:ORG_IS_LOCATED_IN]->(:Place {name: 'Hungary'})
		RETURN
				friend.id AS personId,
				friend.firstName AS personFirstName,
				friend.lastName AS personLastName,
				company.name AS organizationName,
				workAt.workFrom AS organizationWorkFromYear
		ORDER BY
				organizationWorkFromYear ASC,
				personId ASC,
				organizationName DESC
		LIMIT 10" 0
}

run_ldbc_c12() {
	# LDBC IC12 Expert search
	run_query "MATCH (tag:Tag)-[:HAS_TYPE|IS_SUBCLASS_OF*0..]->(baseTagClass:TagClass)
		WHERE tag.name = \"Monarch\" OR baseTagClass.name = \"Monarch\"
		WITH collect(tag.id) as tags
		MATCH (:Person {id: 10995116278009 })-[:KNOWS]-(friend:Person)<-[:HAS_CREATOR]-(comment:Comment)-[:REPLY_OF]->(:Post)-[:HAS_TAG]->(tag:Tag)
		WHERE tag.id in tags
		RETURN
			friend.id AS personId,
			friend.firstName AS personFirstName,
			friend.lastName AS personLastName,
			collect(DISTINCT tag.name) AS tagNames,
			count(DISTINCT comment) AS replyCount
		ORDER BY
			replyCount DESC,
			toInteger(personId) ASC
		LIMIT 20" 1
	run_query "MATCH (tag:Tag)-[:HAS_TYPE*0..5]->(baseTagClass:TagClass)
		WHERE tag.name = 'Hamid_Karzai'
		WITH tag
		MATCH (person:Person)<-[:KNOWS]-(friend:Person)<-[:HAS_CREATOR]-(comment:Comment)-[:REPLY_OF]->(post:Post)-[:POST_HAS_TAG]->(tag)
		RETURN
			friend.id AS personId,
			friend.firstName AS personFirstName,
			friend.lastName AS personLastName,
			count(comment) AS replyCount
		ORDER BY
			replyCount DESC,
			personId ASC
		LIMIT 20" 0
}

run_ldbc_c13() {
	# LDBC IC13 Single shortest path
	run_query "MATCH
		(person1:Person {id: 8796093022390}),
		(person2:Person {id: 8796093022357}),
		path = shortestPath((person1)-[:KNOWS*]-(person2))
		RETURN
			CASE path IS NULL
				WHEN true THEN -1
				ELSE length(path)
			END AS shortestPathLength" 1
}

run_ldbc_c14() {
	# LDBC IC14 Trusted connection paths
	run_query "MATCH path = allShortestPaths((person1:Person { id: 8796093022357 })-[:KNOWS*0..]-(person2:Person { id: 8796093022390 }))
		WITH collect(path) as paths
		UNWIND paths as path
		WITH path, relationships(path) as rels_in_path
		WITH
			[n in nodes(path) | n.id ] as personIdsInPath,
			[r in rels_in_path |
				reduce(w=0.0, v in [
					(a:Person)<-[:HAS_CREATOR]-(:Comment)-[:REPLY_OF]->(:Post)-[:HAS_CREATOR]->(b:Person)
					WHERE
						(a.id = startNode(r).id and b.id=endNode(r).id) OR (a.id=endNode(r).id and b.id=startNode(r).id)
					| 1.0] | w+v)
			] as weight1,
			[r in rels_in_path |
				reduce(w=0.0,v in [
				(a:Person)<-[:HAS_CREATOR]-(:Comment)-[:REPLY_OF]->(:Comment)-[:HAS_CREATOR]->(b:Person)
				WHERE
						(a.id = startNode(r).id and b.id=endNode(r).id) OR (a.id=endNode(r).id and b.id=startNode(r).id)
				| 0.5] | w+v)
			] as weight2
		WITH
			personIdsInPath,
			reduce(w=0.0,v in weight1| w+v) as w1,
			reduce(w=0.0,v in weight2| w+v) as w2
		RETURN
			personIdsInPath,
			(w1+w2) as pathWeight
		ORDER BY pathWeight desc" 1
}

run_ldbc_c() {
	# LDBC Complex
	echo "RUN LDBC Complex"
	echo "Enter queries to run (query_num|All)"
	read -a queries

	for query in "${queries[@]}"; do
		echo "RUN LDBC Complex query" $query
		if [ $query == 'All' ]; then
			run_ldbc_c2
			run_ldbc_c3
			run_ldbc_c4
			run_ldbc_c5
			run_ldbc_c6
			run_ldbc_c7
			run_ldbc_c8
			run_ldbc_c9
			run_ldbc_c10
			run_ldbc_c11
			run_ldbc_c12
		else
			run_ldbc_c${query}
		fi
	done
}

run_ldbc() {
	run_ldbc_s
	run_ldbc_c
}

run_tpch1() {
	# TPC-H Q1 Pricing Summary Report Query
	run_query "MATCH (item:LINEITEM)
		WHERE item.L_SHIPDATE <= date('1998-08-25')
		RETURN
			item.L_RETURNFLAG AS ret_flag,
			item.L_LINESTATUS AS line_stat,
			sum(item.L_QUANTITY) AS sum_qty,
			sum(item.L_EXTENDEDPRICE) AS sum_base_price,
			sum(item.L_EXTENDEDPRICE * (1 - item.L_DISCOUNT)) AS sum_disc_price,
			sum(item.L_EXTENDEDPRICE*(1 - item.L_DISCOUNT)*(1 + item.L_TAX)) AS sum_charge,
			avg(item.L_QUANTITY) AS avg_qty,
			avg(item.L_EXTENDEDPRICE) AS avg_price,
			avg(item.L_DISCOUNT) AS avg_disc,
			COUNT(*) AS count_order
		ORDER BY
			ret_flag,
			line_stat;" 0
}

run_tpch2() {
	# TPC-H Q2 Minimum Cost Supplier Query
	# p2.PS_SUPPLYCOST = minvalue
	run_query "MATCH (r:REGION)-[:IS_LOCATED_IN_BWD]->(:NATION)-[:SUPP_BELONG_TO_BWD]->(:SUPPLIER)-[p:PARTSUPP_BWD]->(pa:PART)
		WHERE r.R_NAME = 'AMERICA'
		WITH min(p.PS_SUPPLYCOST) as minvalue
		MATCH (r2:REGION)-[:IS_LOCATED_IN_BWD]->(n2:NATION)-[:SUPP_BELONG_TO_BWD]->(s2:SUPPLIER)-[p2:PARTSUPP_BWD]->(pa2:PART)
		WHERE pa2.P_SIZE = 43
			AND r2.R_NAME = 'AMERICA'
			AND pa2.P_TYPE CONTAINS 'COPPER'
			AND p2.PS_SUPPLYCOST = minvalue
		RETURN
			s2.S_ACCTBAL,
			s2.S_NAME,
			n2.N_NAME,
			pa2.P_PARTKEY,
			pa2.P_MFGR,
			s2.S_ADDRESS,
			s2.S_PHONE,
			s2.S_COMMENT
		ORDER BY
			s2.S_ACCTBAL DESC,
			n2.N_NAME,
			s2.S_NAME,
			pa2.P_PARTKEY
		LIMIT 100;" 0
}

run_tpch3() {
	# TPC-H Q3 Shipping Priority Query
	run_query "MATCH (l:LINEITEM)-[:IS_PART_OF]->(ord:ORDERS)-[:MADE_BY]->(c:CUSTOMER)
		WHERE c.C_MKTSEGMENT = 'FURNITURE'
			AND ord.O_ORDERDATE < date('1995-03-07')
			AND l.L_SHIPDATE > date('1995-03-07')
		RETURN
			ord.O_ORDERKEY,
			sum(l.L_EXTENDEDPRICE*(1-l.L_DISCOUNT)) AS revenue,
			ord.O_ORDERDATE AS ord_date,
			ord.O_SHIPPRIORITY
		ORDER BY
			revenue DESC,
			ord_date
		LIMIT 10;" 1
	run_query "MATCH (c:CUSTOMER)-[:MADE_BY_BWD]->(ord:ORDERS)-[:IS_PART_OF_BWD]->(l:LINEITEM)
		WHERE c.C_MKTSEGMENT = 'FURNITURE'
			AND ord.O_ORDERDATE < date('1995-03-07')
			AND l.L_SHIPDATE > date('1995-03-07')
		RETURN
			ord.O_ORDERKEY,
			sum(l.L_EXTENDEDPRICE*(1-l.L_DISCOUNT)) AS revenue,
			ord.O_ORDERDATE AS ord_date,
			ord.O_SHIPPRIORITY
		ORDER BY
			revenue DESC,
			ord_date
		LIMIT 10;" 0
}

run_tpch4() {
	# TPC-H Q4 Order Priority Checking Query
	# TODO distinct bug
	run_query "MATCH (l: LINEITEM)-[:IS_PART_OF]->(ord:ORDERS)
		WHERE l.L_COMMITDATE < l.L_RECEIPTDATE 
			AND ord.O_ORDERDATE >= date('1994-01-01')
			AND ord.O_ORDERDATE < date('1994-04-01')
		WITH distinct(ord)
		RETURN
			ord.O_ORDERPRIORITY AS ord_pr,
			COUNT(*) AS ORDER_COUNT
		ORDER BY
			ord_pr;" 1
	run_query "MATCH (l: LINEITEM)<-[:IS_PART_OF_BWD]-(ord:ORDERS)
		WHERE l.L_COMMITDATE < l.L_RECEIPTDATE 
			AND ord.O_ORDERDATE >= date('1994-01-01')
			AND ord.O_ORDERDATE < date('1994-04-01')
		RETURN
			ord.O_ORDERPRIORITY AS ord_pr,
			COUNT(*) AS ORDER_COUNT
		ORDER BY
			ord_pr;" 1
	run_query "MATCH (l: LINEITEM)-[:IS_PART_OF]->(ord:ORDERS)
		WHERE l.L_COMMITDATE < l.L_RECEIPTDATE 
			AND ord.O_ORDERDATE >= date('1994-01-01')
			AND ord.O_ORDERDATE < date('1994-04-01')
		WITH distinct ord
		RETURN
			ord.O_ORDERPRIORITY AS ord_pr;" 0
}

run_tpch5() {
	# TPC-H Q5 Local Supplier Volume Query
	run_query "MATCH (l:LINEITEM)-[:IS_PART_OF]->(ord:ORDERS)-[:MADE_BY]->(c:CUSTOMER)-[:CUST_BELONG_TO]->(n: NATION)-[:IS_LOCATED_IN]->(r:REGION)
		WHERE r.R_NAME = 'ASIA'
			AND ord.O_ORDERDATE >= date('1994-01-01')
			AND ord.O_ORDERDATE < date('1994-04-01')
		RETURN
			n.N_NAME,
			sum(l.L_EXTENDEDPRICE*(1-l.L_DISCOUNT)) AS REVENUE
		ORDER BY
			REVENUE DESC;" 1
	run_query "MATCH (ord:ORDERS)-[:MADE_BY]->(c:CUSTOMER),
			(l:LINEITEM)-[:IS_PART_OF]->(ord),
			(l)-[:SUPPLIED_BY]->(s:SUPPLIER),
			(s)-[:SUPP_BELONG_TO]->(n:NATION),
			(c)-[:CUST_BELONG_TO]->(n)-[:IS_LOCATED_IN]->(r:REGION)
		WHERE r.R_NAME = 'EUROPE'
			AND ord.O_ORDERDATE >= date('1994-01-01')
			AND ord.O_ORDERDATE < date('1995-01-01')
		RETURN
			n.N_NAME,
			sum(l.L_EXTENDEDPRICE*(1-l.L_DISCOUNT)) AS REVENUE
		ORDER BY
			REVENUE DESC;" 1
	run_query "MATCH (r:REGION)-[:IS_LOCATED_IN_BWD]->(n:NATION)-[:CUST_BELONG_TO_BWD]-(c:CUSTOMER)-[:MADE_BY_BWD]->(ord:ORDERS)-[:IS_PART_OF_BWD]->(l:LINEITEM),
			(l)-[:SUPPLIED_BY]->(s:SUPPLIER)-[:SUPP_BELONG_TO]->(n)
		WHERE r.R_NAME = 'EUROPE'
			AND ord.O_ORDERDATE >= date('1994-01-01')
			AND ord.O_ORDERDATE < date('1995-01-01')
		RETURN
			n.N_NAME,
			sum(l.L_EXTENDEDPRICE*(1-l.L_DISCOUNT)) AS REVENUE
		ORDER BY
			REVENUE DESC;" 0
}

run_tpch6() {
	# TPC-H Q6 Forecasting Revenue Change Query
	run_query "MATCH (li:LINEITEM)
		WHERE li.L_SHIPDATE >= date('1994-01-01')
			AND li.L_SHIPDATE < date('1995-01-01')
			AND li.L_DISCOUNT >= 0.05 - 0.01
			AND li.L_DISCOUNT <= 0.05 + 0.01
			AND li.L_QUANTITY < 24
		RETURN
			sum(li.L_EXTENDEDPRICE * li.L_DISCOUNT) as revenue;" 0
}

run_tpch7() {
	# TPC-H Q7 Volume Shipping Query
	# date(li.L_SHIPDATE).year as l_year,
	run_query "MATCH (li:LINEITEM)-[:SUPPLIED_BY]->(s:SUPPLIER)-[:SUPP_BELONG_TO]->(n1:NATION)
		MATCH (li)-[:IS_PART_OF]->(o:ORDERS)-[:MADE_BY]->(c:CUSTOMER)-[:CUST_BELONG_TO]->(n2:NATION) 
		WHERE ((n1.N_NAME = 'IRAN' AND n2.N_NAME = 'ETHIOPIA')
			OR (n1.N_NAME = 'ETHIOPIA' AND n2.N_NAME = 'IRAN'))
			AND li.L_SHIPDATE > date('1995-01-01')
			AND li.L_SHIPDATE < date('1996-12-31')
		RETURN 
			n1.N_NAME AS supp_nation,
			n2.N_NAME AS cust_nation,
			year(li.L_SHIPDATE) AS l_year,
			sum(li.L_EXTENDEDPRICE * (1-li.L_DISCOUNT)) as volume
		ORDER BY
			supp_nation,
			cust_nation,
			l_year;" 1
	run_query "MATCH (li:LINEITEM)<-[:SUPPLIED_BY_BWD]-(s:SUPPLIER)<-[:SUPP_BELONG_TO_BWD]-(n1:NATION)
		MATCH (li)-[:IS_PART_OF]->(o:ORDERS)-[:MADE_BY]->(c:CUSTOMER)-[:CUST_BELONG_TO]->(n2:NATION)
		WHERE ((n1.N_NAME = 'IRAN' AND n2.N_NAME = 'ETHIOPIA')
			OR (n1.N_NAME = 'ETHIOPIA' AND n2.N_NAME = 'IRAN'))
			AND li.L_SHIPDATE > date('1995-01-01')
			AND li.L_SHIPDATE < date('1996-12-31')
		WITH 
			n1.N_NAME AS supp_nation,
			n2.N_NAME AS cust_nation,
			year(li.L_SHIPDATE) AS l_year,
			(li.L_EXTENDEDPRICE * (1-li.L_DISCOUNT)) as volume
		RETURN
			supp_nation, cust_nation, l_year, sum(volume) as revenue
		ORDER BY
			supp_nation,
			cust_nation,
			l_year;" 0
}

run_tpch8() {
	# TPC-H Q8 National Market Share Query
	# date(o.O_ORDERDATE) as o_year
	# / volume 
	run_query "MATCH (li:LINEITEM)-[:COMPOSED_BY]->(p:PART)
		MATCH (li)-[:SUPPLIED_BY]->(s:SUPPLIER)-[:SUPP_BELONG_TO]->(n2:NATION)
		MATCH (li)-[:IS_PART_OF]->(o:ORDERS)-[:MADE_BY]->(c:CUSTOMER)-[:CUST_BELONG_TO]->(n1:NATION)-[:IS_LOCATED_IN]->(r:REGION)
		WHERE r.R_NAME = 'AMERICA'
			AND o.O_ORDERDATE > date('1995-01-01')
			AND o.O_ORDERDATE < date('1996-12-31')
			AND p.P_TYPE = 'ECONOMY ANODIZED STEEL'
		WITH 
			year(o.O_ORDERDATE) AS o_year,
			sum(li.L_EXTENDEDPRICE * (1-li.L_DISCOUNT)) AS volume,
			n2.N_NAME AS nation
		RETURN o_year,
			sum(CASE WHEN nation = 'ETHIOPIA'
				THEN volume
				ELSE 0 END) as mkt_share
		ORDER BY o_year;" 1
	run_query "MATCH (p:PART)-[:COMPOSED_BY_BWD]->(li:LINEITEM)
		MATCH (li)-[:SUPPLIED_BY]->(s:SUPPLIER)-[:SUPP_BELONG_TO]->(n2:NATION)
		MATCH (li)-[:IS_PART_OF]->(o:ORDERS)-[:MADE_BY]->(c:CUSTOMER)-[:CUST_BELONG_TO]->(n1:NATION)-[:IS_LOCATED_IN]->(r:REGION)
		WHERE r.R_NAME = 'AMERICA'
			AND o.O_ORDERDATE > date('1995-01-01')
			AND o.O_ORDERDATE < date('1996-12-31')
			AND p.P_TYPE = 'ECONOMY ANODIZED STEEL'
		WITH
			year(o.O_ORDERDATE) AS o_year, li, n2.N_NAME AS NATION
		WITH
			o_year,
			sum(li.L_EXTENDEDPRICE * (1-li.L_DISCOUNT)) AS volume,
			NATION
		RETURN
			o_year,
			sum(CASE WHEN NATION = 'ETHIOPIA'
				THEN volume
				ELSE 0 END) as mkt_share
		ORDER BY o_year;" 0
}

run_tpch9() {
	# TPC-H Q9 Product Type Profit Measure Query
	run_query "MATCH (o:ORDERS)<-[:IS_PART_OF]-(li:LINEITEM)-[:SUPPLIED_BY]->(s:SUPPLIER)-[:SUPP_BELONG_TO]->(n:NATION)
		MATCH (p:PART)-[ps:PARTSUPP]->(s)
		WHERE p.P_NAME CONTAINS 'salmon'
		RETURN
			n.N_NAME as nation,
			year(o.O_ORDERDATE) as year,
			sum(li.L_EXTENDEDPRICE * (1 -li.L_DISCOUNT) - ps.PS_SUPPLYCOST * li.L_QUANTITY) as amount
		ORDER BY nation DESC, year;" 1
	run_query "MATCH (p:PART)-[:COMPOSED_BY_BWD]->(li:LINEITEM)-[:SUPPLIED_BY]->(s:SUPPLIER),
			(p)-[ps:PARTSUPP]->(s),
			(s)-[:SUPP_BELONG_TO]->(n:NATION),
			(li)-[:IS_PART_OF]->(o:ORDERS)
		WHERE p.P_NAME CONTAINS 'salmon'
		WITH n.N_NAME as nation, year(o.O_ORDERDATE) as year, li.L_EXTENDEDPRICE * (1 -li.L_DISCOUNT) - ps.PS_SUPPLYCOST * li.L_QUANTITY as tmp
		RETURN
			nation,
			year, 
			sum(tmp) as amount
		ORDER BY nation, year desc;" 0
}

run_tpch10() {
	# TPC-H Q10 Returned Item Reporting Query
	run_query "MATCH (li:LINEITEM)-[:IS_PART_OF]->(o:ORDERS)-[:MADE_BY]->(c:CUSTOMER)-[:CUST_BELONG_TO]->(n:NATION)
		WHERE o.O_ORDERDATE >= date('1993-07-01')
			AND o.O_ORDERDATE < date('1993-10-01')
			AND li.L_RETURNFLAG = 'R'
		RETURN
			c.C_CUSTKEY,
			c.C_NAME,
			c.C_ACCTBAL,
			n.N_NAME,
			c.C_ADDRESS,
			c.C_PHONE,
			c.C_COMMENT,
			sum(li.L_EXTENDEDPRICE * (1-li.L_DISCOUNT)) as revenue
		ORDER BY 
			revenue desc
		LIMIT 20;" 1
	run_query "MATCH (li:LINEITEM)<-[:IS_PART_OF_BWD]-(o:ORDERS)<-[:MADE_BY_BWD]-(c:CUSTOMER)<-[:CUST_BELONG_TO_BWD]-(n:NATION)
		WHERE o.O_ORDERDATE >= date('1993-07-01')
			AND o.O_ORDERDATE < date('1993-10-01')
			AND li.L_RETURNFLAG = 'R'
		RETURN
			c.C_CUSTKEY,
			c.C_NAME,
			c.C_ACCTBAL,
			n.N_NAME,
			c.C_ADDRESS,
			c.C_PHONE,
			c.C_COMMENT,
			sum(li.L_EXTENDEDPRICE * (1-li.L_DISCOUNT)) as revenue
		ORDER BY 
			revenue desc
		LIMIT 20;" 0
}

run_tpch11() {
	# TPC-H Q11 Important Stock Identification Query
	# WITH sum(tmp1) * 0.0001 as subquery, sum(tmp2) as value 
	run_query "MATCH (pa:PART)-[p:PARTSUPP]->(s:SUPPLIER)-[:SUPP_BELONG_TO]->(n:NATION)
		WHERE n.N_NAME = 'GERMANY'
		WITH sum(p.PS_SUPPLYCOST * p.PS_AVAILQTY) as subquery, sum(p.PS_SUPPLYCOST * p.PS_AVAILQTY) as value 
		MATCH (pa2:PART)-[:PARTSUPP]->(s2:SUPPLIER)-[:SUPP_BELONG_TO]->(n2:NATION)
		WHERE n2.N_NAME = 'GERMANY' AND value > subquery
		RETURN
			pa2.P_PARTKEY,
			value
		ORDER BY
			value desc;" 1
	run_query "MATCH (pa:PART)<-[p:PARTSUPP_BWD]-(s:SUPPLIER)<-[:SUPP_BELONG_TO_BWD]-(n:NATION)
		WHERE n.N_NAME = 'ROMANIA'
		WITH sum(p.PS_SUPPLYCOST * p.PS_AVAILQTY) as subquery
		MATCH (pa2:PART)<-[p2:PARTSUPP_BWD]-(s2:SUPPLIER)<-[:SUPP_BELONG_TO_BWD]-(n2:NATION)
		WHERE n2.N_NAME = 'ROMANIA'
		WITH pa2.P_PARTKEY AS P_PARTKEY, sum(p2.PS_SUPPLYCOST * p2.PS_AVAILQTY) as value
		RETURN
			P_PARTKEY,
			value
		ORDER BY
			value desc;" 0
	run_query "MATCH (pa:PART)<-[p:PARTSUPP_BWD]-(s:SUPPLIER)<-[:SUPP_BELONG_TO_BWD]-(n:NATION)
		WHERE n.N_NAME = 'ROMANIA'
		WITH sum(p.PS_SUPPLYCOST * p.PS_AVAILQTY) as subquery
		MATCH (pa2:PART)<-[p2:PARTSUPP_BWD]-(s2:SUPPLIER)<-[:SUPP_BELONG_TO_BWD]-(n2:NATION)
		WHERE n2.N_NAME = 'ROMANIA'
		RETURN
			pa2.P_PARTKEY,
			p2.PS_SUPPLYCOST,
			p2.PS_AVAILQTY
		ORDER BY pa2.P_PARTKEY;" 1
}

run_tpch12() {
	# TPC-H Q12 Shipping Modes and Order Priority Query
	run_query "MATCH (li:LINEITEM)-[:IS_PART_OF]->(o:ORDERS)
		WHERE (li.L_SHIPMODE = 'REG AIR' OR li.L_SHIPMODE = 'FOB')
			AND li.L_COMMITDATE < li.L_RECEIPTDATE
			AND li.L_SHIPDATE < li.L_COMMITDATE
			AND li.L_RECEIPTDATE >= date('1997-01-01')
			AND li.L_RECEIPTDATE < date('1998-01-01')
		RETURN 
			li.L_SHIPMODE AS l_shipmode,
			sum(CASE WHEN o.O_ORDERPRIORITY = '1-URGENT' OR o.O_ORDERPRIORITY = '2-HIGH'
				THEN 1
				ELSE 0 END) as high_line_count,
			sum(CASE WHEN o.O_ORDERPRIORITY = '3-MEDIUM' OR o.O_ORDERPRIORITY = '4-NOT SPECIFIED' OR o.O_ORDERPRIORITY = '5-LOW'
				THEN 1
				ELSE 0 END) as low_line_count
		ORDER BY l_shipmode;" 0
}

run_tpch13() {
	# TPC-H Q13 Customer Distribution Query
	# WHERE NOT (order.O_COMMENT =~ '.*special.*.*requests.*')
	run_query "MATCH (ord: ORDERS)
		OPTIONAL MATCH (ord)-[:MADE_BY]->(c: CUSTOMER)
		WHERE NOT (ord.O_COMMENT CONTAINS 'express' AND ord.O_COMMENT CONTAINS 'deposits')
		WITH c.C_CUSTKEY AS c_id, COUNT(ord.O_ORDERKEY) AS c_count
		RETURN
			c_count,
			COUNT(c_id) AS custdist
		ORDER BY
			custdist DESC,
			c_count DESC;" 0
}

run_tpch14() {
	# TPC-H Q14 Promotion Effect Query
	# 100 * SUM(CASE WHEN part.P_TYPE =~ '.*PROMO.*'
	run_query "MATCH (item: LINEITEM)-[:COMPOSED_BY]->(part: PART)
		WHERE item.L_SHIPDATE >= date('1995-09-01') 
			AND item.L_SHIPDATE < date('1995-12-01')
		RETURN
			SUM(CASE WHEN part.P_TYPE CONTAINS 'PROMO'
				THEN item.L_EXTENDEDPRICE * (1 - item.L_DISCOUNT) 
				ELSE 0 END) / SUM(item.L_EXTENDEDPRICE * (1 - item.L_DISCOUNT)) AS PROMO_REVENUE;" 1
	run_query "MATCH (item: LINEITEM)-[:COMPOSED_BY]->(part: PART)
		WHERE item.L_SHIPDATE >= date('1995-09-01') 
			AND item.L_SHIPDATE < date('1995-12-01')
		RETURN
			SUM(CASE WHEN part.P_TYPE CONTAINS 'PROMO'
				THEN item.L_EXTENDEDPRICE * (1 - item.L_DISCOUNT) 
				ELSE 0 END) AS PROMO_REVENUE1,
			SUM(item.L_EXTENDEDPRICE * (1 - item.L_DISCOUNT)) AS PROMO_REVENUE2;" 0
}

run_tpch15() {
	# TPC-H Q15 Top Supplier Query
	run_query "MATCH (li:LINEITEM)-[:SUPPLIED_BY]->(s:SUPPLIER)
		WHERE li.L_SHIPDATE >= date('1995-11-01')
			AND  li.L_SHIPDATE < date('1996-02-01')
		WITH s.S_SUPPKEY as suppkey,
			avg(li.L_EXTENDEDPRICE * (1-li.L_DISCOUNT)) as total_revenue
		MATCH (li2:LINEITEM)-[:SUPPLIED_BY]->(s2:SUPPLIER)
		WHERE li2.L_SHIPDATE >= date('1995-11-01')
			AND li2.L_SHIPDATE < date('1996-02-01')
		WITH s2, sum(li2.L_EXTENDEDPRICE * (1-li2.L_DISCOUNT)) as total_revenue2, total_revenue
		WHERE total_revenue2 = total_revenue
		RETURN
			s2.S_SUPPKEY AS S_SUPPKEY,
			s2.S_NAME,
			s2.S_ADDRESS,
			s2.S_PHONE,
			total_revenue2
		ORDER BY S_SUPPKEY;" 1
	run_query "MATCH (li:LINEITEM)-[:SUPPLIED_BY]->(s:SUPPLIER)
		WHERE li.L_SHIPDATE >= date('1995-11-01')
			AND li.L_SHIPDATE < date('1996-02-01')
		WITH s.S_SUPPKEY as suppkey, sum(li.L_EXTENDEDPRICE * (1-li.L_DISCOUNT)) as total_revenue
		WITH max(total_revenue) as max_total_revenue
		MATCH (li2:LINEITEM)-[:SUPPLIED_BY]->(s2:SUPPLIER)
		WHERE li2.L_SHIPDATE >= date('1995-11-01')
			AND li2.L_SHIPDATE < date('1996-02-01')
		WITH
			s2.S_SUPPKEY AS S_SUPPKEY,
			s2.S_NAME AS S_NAME,
			s2.S_ADDRESS AS S_ADDRESS,
			s2.S_PHONE AS S_PHONE,
			max_total_revenue,
			sum(li2.L_EXTENDEDPRICE * (1-li2.L_DISCOUNT)) as total_revenue2
		WHERE total_revenue2 = max_total_revenue
		RETURN
			S_SUPPKEY,
			S_NAME,
			S_ADDRESS,
			S_PHONE,
			total_revenue2,
			max_total_revenue
		ORDER BY S_SUPPKEY;" 0
}

run_tpch16() {
	# TPC-H Q16 Parts/Supplier Relationship Query
	# 			AND p.P_SIZE in [49,14,23,45,19,3,36,9]
	run_query "MATCH (s:SUPPLIER)
		WHERE s.S_COMMENT CONTAINS 'Complaints'
		WITH s.S_SUPPKEY as p_id
		MATCH (s2:SUPPLIER)<-[:PARTSUPP]-(p:PART)
		WHERE p.P_BRAND <> 'Brand#45' 
			AND NOT (p.P_TYPE CONTAINS 'MEDIUM POLISHED')
			AND NOT s2.S_SUPPKEY = p_id
		RETURN 
			p.P_BRAND AS P_BRAND,
			p.P_TYPE AS P_TYPE,
			p.P_SIZE AS P_SIZE,
			count(distinct s2.S_SUPPKEY) as supplier_cnt
		ORDER BY
			supplier_cnt desc,
			P_BRAND,
			P_TYPE,
			P_SIZE;" 1
	run_query "MATCH (s:SUPPLIER)<-[:PARTSUPP]-(p:PART)
		WHERE NOT s.S_COMMENT CONTAINS 'Complaints'
			AND p.P_BRAND <> 'Brand#45'
			AND NOT (p.P_TYPE CONTAINS 'MEDIUM POLISHED')
			AND ((p.P_SIZE = 49)
				OR (p.P_SIZE = 14)
				OR (p.P_SIZE = 23)
				OR (p.P_SIZE = 45)
				OR (p.P_SIZE = 19)
				OR (p.P_SIZE = 3)
				OR (p.P_SIZE = 36)
				OR (p.P_SIZE = 9))
		RETURN
			p.P_BRAND,
			p.P_TYPE,
			p.P_SIZE,
			count(s.S_SUPPKEY) as supplier_cnt" 1
	run_query "MATCH (s:SUPPLIER)<-[:PARTSUPP]-(p:PART)
		WHERE NOT s.S_COMMENT CONTAINS 'Complaints'
			AND p.P_BRAND <> 'Brand#51'
			AND NOT (p.P_TYPE CONTAINS 'PROMO PLATED')
			AND ((p.P_SIZE = 11)
				OR (p.P_SIZE = 44)
				OR (p.P_SIZE = 42)
				OR (p.P_SIZE = 8)
				OR (p.P_SIZE = 45)
				OR (p.P_SIZE = 14)
				OR (p.P_SIZE = 40)
				OR (p.P_SIZE = 46))
		RETURN
			p.P_BRAND as P_BRAND,
			p.P_TYPE as P_TYPE,
			p.P_SIZE as P_SIZE,
			count(s.S_SUPPKEY) as supplier_cnt
		ORDER BY
			supplier_cnt desc, P_BRAND, P_TYPE, P_SIZE;" 0
}

run_tpch17() {
	# TPC-H Q17 Small-Quantity-Order Revenue Query
	# 		WITH 0.2 * lineitem.L_QUANTITY AS tmp1
	# SUM(item.L_EXTENDEDPRICE) /7.0 AS avg_yearly;" 0
	# remove part2.P_BRAND, part2.P_CONTAINER
	run_query "MATCH (lineitem: LINEITEM)<-[:COMPOSED_BY_BWD]-(part:PART)
		WHERE part.P_BRAND = 'Brand#15'
			AND part.P_CONTAINER = 'LG CASE'
		WITH avg(lineitem.L_QUANTITY) AS L_QUANTITY
		MATCH (item: LINEITEM)<-[:COMPOSED_BY_BWD]-(part2:PART)
		WHERE part2.P_BRAND = 'Brand#15'
			AND part2.P_CONTAINER = 'LG CASE'
			AND item.L_QUANTITY < L_QUANTITY
		RETURN
			SUM(item.L_EXTENDEDPRICE) AS avg_yearly;" 1
	run_query "MATCH (lineitem: LINEITEM)<-[:COMPOSED_BY_BWD]-(part:PART)
		WHERE part.P_BRAND = 'Brand#15'
			AND part.P_CONTAINER = 'LG CASE'
		WITH 0.2 * avg(lineitem.L_QUANTITY) AS avg_quantity
		MATCH (item: LINEITEM)<-[:COMPOSED_BY_BWD]-(part2:PART)
		WHERE part2.P_BRAND = 'Brand#15'
			AND part2.P_CONTAINER = 'LG CASE'
			AND item.L_QUANTITY < avg_quantity
		RETURN
			SUM(item.L_EXTENDEDPRICE) AS avg_yearly;" 0
}

run_tpch18() {
	# TPC-H Q18 Large Volume Customer Query
	# WHERE ord.O_ORDERKEY IN [l_orderkey]
	run_query "MATCH (lineitem: LINEITEM)
		WITH lineitem.L_ORDERKEY as l_orderkey, sum(lineitem.L_QUANTITY) AS sum_lquantity
		WHERE sum_lquantity > 315
		MATCH (item:LINEITEM)-[:IS_PART_OF]->(ord: ORDERS)-[:MADE_BY]->(customer: CUSTOMER)
		WHERE item.L_ORDERKEY = l_orderkey
		RETURN
			customer.C_NAME,
			customer.C_CUSTKEY,
			ord.O_ORDERKEY,
			ord.O_ORDERDATE AS orderdate,
			ord.O_TOTALPRICE AS totalprice,
			SUM(item.L_QUANTITY)
		ORDER BY
			totalprice DESC,
			orderdate
		LIMIT 100;" 0
}

run_tpch19() {
	# TPC-H Q19 Discounted Revenue Query
	run_query "MATCH (part:PART)-[:COMPOSED_BY_BWD]->(lineitem: LINEITEM)
		WHERE (part.P_BRAND = 'Brand#25'
			AND (part.P_CONTAINER = 'SM CASE' OR part.P_CONTAINER = 'SM BOX' OR part.P_CONTAINER = 'SM PACK' OR part.P_CONTAINER = 'SM PKG')
			AND lineitem.L_QUANTITY >= 8 AND lineitem.L_QUANTITY <= 8 + 10
			AND part.P_SIZE >= 1 AND part.P_SIZE <= 5
			AND (lineitem.L_SHIPMODE = 'AIR' OR lineitem.L_SHIPMODE = 'AIR REG')
			AND lineitem.L_SHIPINSTRUCT = 'DELIVER IN PERSON')
		OR (part.P_BRAND = 'Brand#51'
			AND (part.P_CONTAINER = 'MED BAG' OR part.P_CONTAINER = 'MED BOX' OR part.P_CONTAINER = 'MED PACK' OR part.P_CONTAINER = 'MED PKG')
			AND lineitem.L_QUANTITY >= 19 AND lineitem.L_QUANTITY <= 19 + 10
			AND part.P_SIZE >= 1 AND part.P_SIZE <= 10
			AND (lineitem.L_SHIPMODE = 'AIR' OR lineitem.L_SHIPMODE = 'AIR REG')
			AND lineitem.L_SHIPINSTRUCT = 'DELIVER IN PERSON')
		OR (part.P_BRAND = 'Brand#51'
			AND (part.P_CONTAINER = 'LG CASE' OR part.P_CONTAINER = 'LG BOX' OR part.P_CONTAINER = 'LG PACK' OR part.P_CONTAINER = 'LG PKG')
			AND lineitem.L_QUANTITY >= 29 AND lineitem.L_QUANTITY <= 29 + 10
			AND part.P_SIZE >= 1 AND part.P_SIZE <= 15
			AND (lineitem.L_SHIPMODE = 'AIR' OR lineitem.L_SHIPMODE = 'AIR REG')
			AND lineitem.L_SHIPINSTRUCT = 'DELIVER IN PERSON')
		RETURN
			SUM(lineitem.L_EXTENDEDPRICE * (1 - lineitem.L_DISCOUNT)) AS revenue;" 0
}

run_tpch20() {
	# TPC-H Q20 Potential Part Promotion Query
	# WITH ps, s, 0.5 * sum(li.L_QUANTITY) as quantity_sum
	# CONTAINS -> STARTS WITH
	# load_eid WHERE ps.PS_AVAILQTY > quantity_sum
	run_query "MATCH (s:SUPPLIER)-[:SUPP_BELONG_TO]->(n:NATION),
			(p:PART)-[ps:PARTSUPP]->(s),
			(li:LINEITEM)-[:COMPOSED_BY]->(p),
			(li)-[:SUPPLIED_BY]->(s)
		WHERE n.N_NAME = 'BRAZIL'
			AND p.P_NAME CONTAINS 'smoke'
			AND li.L_SHIPDATE >= date('1997-01-01')
			AND li.L_SHIPDATE < date('1998-01-01')
		RETURN
			s.S_NAME, s.S_ADDRESS" 1
	run_query "MATCH (n:NATION)-[:SUPP_BELONG_TO_BWD]->(s:SUPPLIER)-[:SUPPLIED_BY_BWD]->(li:LINEITEM)-[:COMPOSED_BY]->(p:PART),
			(p)-[ps:PARTSUPP]->(s)
		WHERE n.N_NAME = 'BRAZIL'
			AND p.P_NAME CONTAINS 'smoke'
			AND li.L_SHIPDATE >= date('1997-01-01')
			AND li.L_SHIPDATE < date('1998-01-01')
		WITH ps, s.S_NAME AS S_NAME, s.S_ADDRESS AS S_ADDRESS, sum(li.L_QUANTITY) as quantity_sum
		RETURN
			S_NAME, S_ADDRESS
		ORDER BY S_NAME;" 0
}

run_tpch21() {
	# TPC-H Q21 Suppliers Who Kept Orders Waiting Query
	run_query "MATCH (l1:LINEITEM)-[:SUPPLIED_BY]->(s:SUPPLIER),
			(l1)-[:IS_PART_OF]->(o:ORDERS), 
			(s)-[:SUPP_BELONG_TO]->(n:NATION)
		WHERE n.N_NAME = 'SAUDI ARABIA'
			AND l1.L_RECEIPTDATE > l1.L_COMMITDATE
			AND o.O_ORDERSTATUS = 'F'
		RETURN s.S_NAME AS S_NAME, COUNT(*) AS numwait
		ORDER BY numwait desc, S_NAME
		LIMIT 100;" 1
	run_query "MATCH (n:NATION)-[:SUPP_BELONG_TO_BWD]->(s:SUPPLIER)-[:SUPPLIED_BY_BWD]->(l1:LINEITEM)-[:IS_PART_OF]->(o:ORDERS)
		WHERE n.N_NAME = 'SAUDI ARABIA'
			AND l1.L_RECEIPTDATE > l1.L_COMMITDATE
			AND o.O_ORDERSTATUS = 'F'
		RETURN s.S_NAME AS S_NAME, COUNT(*) AS numwait
		ORDER BY numwait desc, S_NAME
		LIMIT 100;" 1
	run_query "MATCH (n:NATION)-[:SUPP_BELONG_TO_BWD]->(s:SUPPLIER)-[:SUPPLIED_BY_BWD]->(l1:LINEITEM)-[:IS_PART_OF]->(o:ORDERS)
		WHERE n.N_NAME = 'SAUDI ARABIA'
			AND l1.L_RECEIPTDATE > l1.L_COMMITDATE
			AND o.O_ORDERSTATUS = 'F'
			AND EXISTS {
				MATCH (s2:SUPPLIER)-[:SUPPLIED_BY_BWD]->(l2:LINEITEM)-[:IS_PART_OF]->(o)
				WHERE s.S_SUPPKEY <> s2.S_SUPPKEY
			}
			AND NOT EXISTS {
				MATCH (s3:SUPPLIER)-[:SUPPLIED_BY_BWD]->(l3:LINEITEM)-[:IS_PART_OF]->(o)
				WHERE s.S_SUPPKEY <> s3.S_SUPPKEY
					AND l3.L_RECEIPTDATE > l3.L_COMMITDATE
			}
		RETURN s.S_NAME AS S_NAME, COUNT(*) AS numwait
		ORDER BY numwait desc, S_NAME
		LIMIT 100;" 1
	run_query "MATCH (n:NATION)-[:SUPP_BELONG_TO_BWD]->(s:SUPPLIER)-[:SUPPLIED_BY_BWD]->(l1:LINEITEM)-[:IS_PART_OF]->(o:ORDERS)
		WHERE n.N_NAME = 'SAUDI ARABIA'
			AND l1.L_RECEIPTDATE > l1.L_COMMITDATE
			AND o.O_ORDERSTATUS = 'F'
			AND EXISTS {
				MATCH (s2:SUPPLIER)-[:SUPPLIED_BY_BWD]->(l2:LINEITEM)-[:IS_PART_OF]->(o)
			}
		RETURN s.S_NAME AS S_NAME, COUNT(*) AS numwait
		ORDER BY numwait desc, S_NAME
		LIMIT 100;" 0
	# exists
	run_query "MATCH (n:NATION)-[:SUPP_BELONG_TO_BWD]->(s:SUPPLIER)-[:SUPPLIED_BY_BWD]->(l1:LINEITEM)-[:IS_PART_OF]->(o:ORDERS)
		WHERE n.N_NAME = 'SAUDI ARABIA'
			AND l1.L_RECEIPTDATE > l1.L_COMMITDATE
			AND o.O_ORDERSTATUS = 'F'
		WITH o, s
		MATCH (o)-[:IS_PART_OF_BWD]->(l2:LINEITEM)-[:SUPPLIED_BY]->(s2:SUPPLIER)
		WHERE s.S_SUPPKEY <> s2.S_SUPPKEY
		WITH distinct o, s
		RETURN s.S_NAME AS S_NAME, COUNT(*) AS numwait
		ORDER BY numwait desc, S_NAME
		LIMIT 100;" 1
	# not exists
	run_query "MATCH (n:NATION)-[:SUPP_BELONG_TO_BWD]->(s:SUPPLIER)-[:SUPPLIED_BY_BWD]->(l1:LINEITEM)-[:IS_PART_OF]->(o:ORDERS)
		WHERE n.N_NAME = 'SAUDI ARABIA'
			AND l1.L_RECEIPTDATE > l1.L_COMMITDATE
			AND o.O_ORDERSTATUS = 'F'
		WITH o, s
		MATCH (o)-[:IS_PART_OF_BWD]->(l2:LINEITEM)-[:SUPPLIED_BY]->(s2:SUPPLIER)
		WHERE s.S_SUPPKEY <> s2.S_SUPPKEY
		WITH distinct o, s
		RETURN s.S_NAME AS S_NAME, COUNT(*) AS numwait
		ORDER BY numwait desc, S_NAME
		LIMIT 100;" 1
	run_query "MATCH (n:NATION)-[:SUPP_BELONG_TO_BWD]->(s:SUPPLIER)-[:SUPPLIED_BY_BWD]->(l1:LINEITEM)-[:IS_PART_OF]->(o:ORDERS)
		WHERE n.N_NAME = 'SAUDI ARABIA'
			AND l1.L_RECEIPTDATE > l1.L_COMMITDATE
			AND o.O_ORDERSTATUS = 'F'
		RETURN s.S_SUPPKEY AS S_SUPPKEY, s.S_NAME, COUNT(*) AS numwait
        ORDER BY numwait desc;" 1
}

run_tpch22() {
	# TPC-H Q22 Global Sales Opportunity Query
	# WHERE NOT EXISTS { MATCH (o:ORDERS)-[:MADE_BY]->(c2) }
	run_query "MATCH (c1:CUSTOMER)
		WHERE c1.C_ACCTBAL > 0.00
			AND (substring(c1.C_PHONE,1,2) = '12'
				OR substring(c1.C_PHONE,1,2) = '32'
				OR substring(c1.C_PHONE,1,2) = '16'
				OR substring(c1.C_PHONE,1,2) = '29'
				OR substring(c1.C_PHONE,1,2) = '11'
				OR substring(c1.C_PHONE,1,2) = '14'
				OR substring(c1.C_PHONE,1,2) = '27')
		WITH avg(c1.C_ACCTBAL) as avg_bal
		MATCH (c2:CUSTOMER)
		WHERE NOT EXISTS {
				MATCH (o:ORDERS)-[:MADE_BY]->(c2)
			}
			AND (substring(c2.C_PHONE,1,2) = '12'
				OR substring(c2.C_PHONE,1,2) = '32'
				OR substring(c2.C_PHONE,1,2) = '16'
				OR substring(c2.C_PHONE,1,2) = '29'
				OR substring(c2.C_PHONE,1,2) = '11'
				OR substring(c2.C_PHONE,1,2) = '14'
				OR substring(c2.C_PHONE,1,2) = '27')
			AND c2.C_ACCTBAL > avg_bal
		WITH c2.C_ACCTBAL AS acctbal, substring(c2.C_PHONE,1,2) AS cntrycode
		RETURN
			cntrycode,
			COUNT(*) as numcust,
			sum(acctbal) as totacctbal;" 1
	run_query "MATCH (c1:CUSTOMER)
		WHERE c1.C_ACCTBAL > 0.00
			AND (substring(c1.C_PHONE,1,2) = '12'
				OR substring(c1.C_PHONE,1,2) = '32'
				OR substring(c1.C_PHONE,1,2) = '16'
				OR substring(c1.C_PHONE,1,2) = '29'
				OR substring(c1.C_PHONE,1,2) = '11'
				OR substring(c1.C_PHONE,1,2) = '14'
				OR substring(c1.C_PHONE,1,2) = '27')
		WITH avg(c1.C_ACCTBAL) as avg_bal
		MATCH (c2:CUSTOMER)
		WHERE (substring(c2.C_PHONE,1,2) = '12'
				OR substring(c2.C_PHONE,1,2) = '32'
				OR substring(c2.C_PHONE,1,2) = '16'
				OR substring(c2.C_PHONE,1,2) = '29'
				OR substring(c2.C_PHONE,1,2) = '11'
				OR substring(c2.C_PHONE,1,2) = '14'
				OR substring(c2.C_PHONE,1,2) = '27')
			AND c2.C_ACCTBAL > avg_bal
		WITH c2.C_ACCTBAL AS acctbal, substring(c2.C_PHONE,1,2) AS cntrycode
		RETURN
			cntrycode,
			COUNT(*) as numcust,
			sum(acctbal) as totacctbal;" 0
}

run_tpch() {
	# TPC-H
	echo "RUN TPC-H"
	echo "Enter queries to run (query_num|All)"
	read -a queries

	for query in "${queries[@]}"; do
		echo "RUN TPC-H query" $query
		if [ $query == 'All' ]; then
			run_tpch1
		else
			run_tpch${query}
		fi
	done
}

if [ $workload_type == "ldbc" ]; then
	run_ldbc
elif [ $workload_type == "ldbc_s" ]; then
	run_ldbc_s
elif [ $workload_type == "ldbc_c" ]; then
	run_ldbc_c
elif [ $workload_type == "tpch" ]; then
	run_tpch
fi
