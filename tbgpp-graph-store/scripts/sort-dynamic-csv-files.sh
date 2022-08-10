#!/bin/bash

basedir=$1

vertex=(Comment.csv Person.csv Forum.csv Post.csv)
edge=(Comment_hasCreator_Person.csv Comment_hasTag_Tag.csv Comment_isLocatedIn_Country.csv Comment_replyOf_Comment.csv Comment_replyOf_Post.csv Forum_containerOf_Post.csv Forum_hasMember_Person.csv Forum_hasModerator_Person.csv Forum_hasTag_Tag.csv Person_hasInterest_Tag.csv Person_isLocatedIn_City.csv Person_knows_Person.csv Person_likes_Comment.csv Person_likes_Post.csv Person_studyAt_University.csv Person_workAt_Company.csv Post_hasCreator_Person.csv Post_hasTag_Tag.csv Post_isLocatedIn_Country.csv)

for i in "${vertex[@]}"; do
	file_header=`(head -n 1 "${basedir}/$i.original")`
	echo $file_header >> ${basedir}/$i
	tail -n+2 "${basedir}/$i.original" | sort --field-separator='|' --key=2 -n >> ${basedir}/$i
done
for i in "${edge[@]}"; do
	file_header=`(head -n 1 "${basedir}/$i.original")`
	echo $file_header >> ${basedir}/$i
	tail -n+2 "${basedir}/$i.original" | sort --field-separator='|' --key=2,3 -n >> ${basedir}/$i
done
