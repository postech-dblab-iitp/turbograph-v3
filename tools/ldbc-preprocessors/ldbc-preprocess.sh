#!/bin/bash

basedir=$1
dynamic_dir=${basedir}/dynamic
static_dir=${basedir}/static

# Change file names
current_dir=$(pwd)
cp ${current_dir}/ldbc_change_file_name_dynamic.sh ${dynamic_dir}
cd ${dynamic_dir}; bash ${dynamic_dir}/ldbc_change_file_name_dynamic.sh

cd ${static_dir}
cp ${current_dir}/ldbc_change_file_name_static.sh ${static_dir}
cd ${static_dir}; bash ${static_dir}/ldbc_change_file_name_static.sh

echo "Dynamic Data Preprocessing..."

# Generate backward edge data
cat ${dynamic_dir}/Comment_hasCreator_Person.csv | sort -t '|' -n -k 2 -k 1 | awk -F '|' '{print $2"|"$1}' > ${dynamic_dir}/Comment_hasCreator_Person.csv.backward
cat ${dynamic_dir}/Comment_hasTag_Tag.csv | sort -t '|' -n -k 2 -k 1 | awk -F '|' '{print $2"|"$1}' > ${dynamic_dir}/Comment_hasTag_Tag.csv.backward
cat ${dynamic_dir}/Comment_isLocatedIn_Place.csv | sort -t '|' -n -k 2 -k 1 | awk -F '|' '{print $2"|"$1}' > ${dynamic_dir}/Comment_isLocatedIn_Place.csv.backward
cat ${dynamic_dir}/Comment_replyOf_Comment.csv | sort -t '|' -n -k 2 -k 1 | awk -F '|' '{print $2"|"$1}' > ${dynamic_dir}/Comment_replyOf_Comment.csv.backward
cat ${dynamic_dir}/Comment_replyOf_Post.csv | sort -t '|' -n -k 2 -k 1 | awk -F '|' '{print $2"|"$1}' > ${dynamic_dir}/Comment_replyOf_Post.csv.backward
cat ${dynamic_dir}/Forum_containerOf_Post.csv | sort -t '|' -n -k 2 -k 1 | awk -F '|' '{print $2"|"$1}' > ${dynamic_dir}/Forum_containerOf_Post.csv.backward
cat ${dynamic_dir}/Forum_hasMember_Person.csv | sort -t '|' -n -k 2 -k 1 -k 3 | awk -F '|' '{print $2"|"$1"|"$3}' > ${dynamic_dir}/Forum_hasMember_Person.csv.backward
cat ${dynamic_dir}/Forum_hasModerator_Person.csv | sort -t '|' -n -k 2 -k 1 | awk -F '|' '{print $2"|"$1}' > ${dynamic_dir}/Forum_hasModerator_Person.csv.backward
cat ${dynamic_dir}/Forum_hasTag_Tag.csv | sort -t '|' -n -k 2 -k 1 | awk -F '|' '{print $2"|"$1}' > ${dynamic_dir}/Forum_hasTag_Tag.csv.backward
cat ${dynamic_dir}/Person_hasInterest_Tag.csv | sort -t '|' -n -k 2 -k 1 | awk -F '|' '{print $2"|"$1}' > ${dynamic_dir}/Person_hasInterest_Tag.csv.backward
cat ${dynamic_dir}/Person_isLocatedIn_Place.csv | sort -t '|' -n -k 2 -k 1 | awk -F '|' '{print $2"|"$1}' > ${dynamic_dir}/Person_isLocatedIn_Place.csv.backward
cat ${dynamic_dir}/Person_knows_Person.csv | sort -t '|' -n -k 2 -k 1 -k 3 | awk -F '|' '{print $2"|"$1"|"$3}' > ${dynamic_dir}/Person_knows_Person.csv.backward
cat ${dynamic_dir}/Person_likes_Comment.csv | sort -t '|'-n -k 2 -k 1 -k 3 | awk -F '|' '{print $2"|"$1"|"$3}' > ${dynamic_dir}/Person_likes_Comment.csv.backward
cat ${dynamic_dir}/Person_likes_Post.csv | sort -t '|' -n -k 2 -k 1 -k 3 | awk -F '|' '{print $2"|"$1"|"$3}' > ${dynamic_dir}/Person_likes_Post.csv.backward
cat ${dynamic_dir}/Person_studyAt_Organisation.csv | sort -t '|' -n -k 2 -k 1 -k 3 | awk -F '|' '{print $2"|"$1"|"$3}' > ${dynamic_dir}/Person_studyAt_Organisation.csv.backward
cat ${dynamic_dir}/Person_workAt_Organisation.csv | sort -t '|' -n -k 2 -k 1 -k 3 | awk -F '|' '{print $2"|"$1"|"$3}' > ${dynamic_dir}/Person_workAt_Organisation.csv.backward
cat ${dynamic_dir}/Post_hasCreator_Person.csv | sort -t '|' -n -k 2 -k 1 | awk -F '|' '{print $2"|"$1}' > ${dynamic_dir}/Post_hasCreator_Person.csv.backward
cat ${dynamic_dir}/Post_hasTag_Tag.csv | sort -t '|' -n -k 2 -k 1 | awk -F '|' '{print $2"|"$1}' > ${dynamic_dir}/Post_hasTag_Tag.csv.backward
cat ${dynamic_dir}/Post_isLocatedIn_Place.csv | sort -t '|' -n -k 2 -k 1 | awk -F '|' '{print $2"|"$1}' > ${dynamic_dir}/Post_isLocatedIn_Place.csv.backward

echo "Static Data Processing..."

# Generate backward edge data
cat ${static_dir}/Organisation_isLocatedIn_Place.csv | sort -t '|' -n -k 2 -k 1 | awk -F '|' '{print $2"|"$1}' > ${static_dir}/Organisation_isLocatedIn_Place.csv.backward
cat ${static_dir}/Place_isPartOf_Place.csv | sort -t '|' -n -k 2 -k 1 | awk -F '|' '{print $2"|"$1}' > ${static_dir}/Place_isPartOf_Place.csv.backward
cat ${static_dir}/TagClass_isSubclassOf_TagClass.csv | sort -t '|' -n -k 2 -k 1 | awk -F '|' '{print $2"|"$1}' > ${static_dir}/TagClass_isSubclassOf_TagClass.csv.backward
cat ${static_dir}/Tag_hasType_TagClass.csv | sort -t '|' -n -k 2 -k 1 | awk -F '|' '{print $2"|"$1}' > ${static_dir}/Tag_hasType_TagClass.csv.backward

echo "Person Knows Person FWD BWD Merging..."

cd ${current_dir}; python3 merge-fwd-bwd-edges.py ${dynamic_dir}