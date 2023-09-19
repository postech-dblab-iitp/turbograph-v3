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

	echo $query_str
	./build_debug/tbgpp-client/TurboGraph-S62 --workspace:${workspace} --query:"$query_str" ${debug_plan_option} --index-join-only --explain ${iterations}
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
			mod.lastName AS moderatorLastName"

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

run_ldbc_c() {
	# LDBC Complex
	echo "RUN LDBC Complex"

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
		LIMIT 20" 1

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
		RETURN person.id, countryX.name, countryY.name, city.name" 1
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
		LIMIT 10" 1

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
		LIMIT 20" 1

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

	# currently distinct deleted
	# WHERE NOT t = tag	
	# WHERE NOT person=friend
	# run_query "MATCH (knownTag:Tag { name: 'Carl_Gustaf_Emil_Mannerheim' })
	# 	WITH knownTag.id as knownTagId
	# 	MATCH (person:Person { id: 4398046511333 })-[:KNOWS*1..2]-(friend:Person)
	# 	WITH DISTINCT knownTagId, friend
	# 	MATCH (friend)<-[phc:POST_HAS_CREATOR]-(post:Post)
	# 	RETURN
	# 		count(post) as postCount
	# 	ORDER BY
	# 		postCount DESC
	# 	LIMIT 10" 1
	run_query "MATCH (knownTag:Tag { name: 'Carl_Gustaf_Emil_Mannerheim' })
		WITH knownTag.id as knownTagId
		MATCH (person:Person { id: 4398046511333 })
		RETURN knownTagId" 1
<<<<<<< HEAD
=======
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
		LIMIT 10" 1
		#RETURN person._id, person.id, friend._id, friend.id, knownTagId, post._id, t._id, tag._id" 0
>>>>>>> 8d3d5bbe7aba44ff6177da76893d7f3eb9067554

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
		LIMIT 20" 1
	
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
		LIMIT 20" 1

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
		LIMIT 20" 1

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
	LIMIT 10" 1

	# LDBC IC11 Job referral
		#WHERE person._id <> friend._id
		#WHERE workAt.workFrom < 2011
	run_query "MATCH (person:Person {id: 10995116277918 })-[:KNOWS*1..2]-(friend:Person)
		WITH DISTINCT friend
		MATCH (friend)-[workAt:WORK_AT]->(company:Organisation {label: Company})-[:ORG_IS_LOCATED_IN]->(:Place {name: Hungary})
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
		LIMIT 20" 1

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
