#!/bin/bash

basedir=$1

list=("static/Organisation id:ID(Organisation)|label:STRING|name:STRING|url:STRING"
"static/Place id:ID(Place)|name:STRING|url:STRING|label:STRING"
"static/TagClass id:ID(TagClass)|name:STRING|url:STRING"
"static/Tag id:ID(Tag)|name:STRING|url:STRING"
"static/TagClass_isSubclassOf_TagClass :START_ID(TagClass)|:END_ID(TagClass)"
"static/Tag_hasType_TagClass :START_ID(Tag)|:END_ID(TagClass)"
"static/Organisation_isLocatedIn_Place :START_ID(Organisation)|:END_ID(Place)"
"static/Place_isPartOf_Place :START_ID(Place)|:END_ID(Place)"
"dynamic/Comment creationDate:LONG|id:ID(Comment)|locationIP:STRING|browserUsed:STRING|content:STRING|length:INT"
"dynamic/Forum creationDate:LONG|id:ID(Forum)|title:STRING"
"dynamic/Person creationDate:LONG|id:ID(Person)|firstName:STRING|lastName:STRING|gender:STRING|birthday:LONG|locationIP:STRING|browserUsed:STRING|speaks:STRING[]|email:STRING[]"
"dynamic/Post creationDate:LONG|id:ID(Post)|imageFile:STRING|locationIP:STRING|browserUsed:STRING|language:STRING|content:STRING|length:INT"
"dynamic/Comment_hasCreator_Person creationDate:LONG|:START_ID(Comment)|:END_ID(Person)"
"dynamic/Comment_isLocatedIn_Country creationDate:LONG|:START_ID(Comment)|:END_ID(Place)"
"dynamic/Comment_replyOf_Comment creationDate:LONG|:START_ID(Comment)|:END_ID(Comment)"
"dynamic/Comment_replyOf_Post creationDate:LONG|:START_ID(Comment)|:END_ID(Post)"
"dynamic/Forum_containerOf_Post creationDate:LONG|:START_ID(Forum)|:END_ID(Post)"
"dynamic/Forum_hasMember_Person creationDate:LONG|:START_ID(Forum)|:END_ID(Person)"
"dynamic/Forum_hasModerator_Person creationDate:LONG|:START_ID(Forum)|:END_ID(Person)"
"dynamic/Forum_hasTag_Tag creationDate:LONG|:START_ID(Forum)|:END_ID(Tag)"
"dynamic/Person_hasInterest_Tag creationDate:LONG|:START_ID(Person)|:END_ID(Tag)"
"dynamic/Person_isLocatedIn_City creationDate:LONG|:START_ID(Person)|:END_ID(Place)"
"dynamic/Person_knows_Person creationDate:LONG|:START_ID(Person)|:END_ID(Person)"
"dynamic/Person_likes_Comment creationDate:LONG|:START_ID(Person)|:END_ID(Comment)"
"dynamic/Person_likes_Post creationDate:LONG|:START_ID(Person)|:END_ID(Post)"
"dynamic/Person_studyAt_University creationDate:LONG|:START_ID(Person)|:END_ID(Organisation)|classYear:INT"
"dynamic/Person_workAt_Company creationDate:LONG|:START_ID(Person)|:END_ID(Organisation)|workFrom:INT"
"dynamic/Post_hasCreator_Person creationDate:LONG|:START_ID(Post)|:END_ID(Person)"
"dynamic/Comment_hasTag_Tag creationDate:LONG|:START_ID(Comment)|:END_ID(Tag)"
"dynamic/Post_hasTag_Tag creationDate:LONG|:START_ID(Post)|:END_ID(Tag)"
"dynamic/Post_isLocatedIn_Country creationDate:LONG|:START_ID(Post)|:END_ID(Place)")
#"dynamic/Person_studyAt_Organisation :START_ID(Person)|:END_ID(Organisation)|classYear:INT"
#"dynamic/Person_workAt_Organisation :START_ID(Person)|:END_ID(Organisation)|workFrom:INT"

for ((i = 0; i < ${#list[@]}; i++)); do
	IFS=' ' read -ra array <<< "${list[$i]}"
	sed -i '1s/^/'${array[1]}'\n/' "${basedir}/${array[0]}.csv"
done
