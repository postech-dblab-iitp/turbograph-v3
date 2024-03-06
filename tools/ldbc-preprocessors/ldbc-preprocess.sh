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

# In place sort node data, skipping the header
cd ${current_dir}
bash sort-convert-node.sh ${dynamic_dir}/Comment.csv
bash sort-convert-node.sh ${dynamic_dir}/Forum.csv
bash sort-convert-node.sh ${dynamic_dir}/Person.csv
bash sort-convert-node.sh ${dynamic_dir}/Post.csv

# In place sort fwd edge data 
bash sort-convert-edge.sh ${dynamic_dir}/Comment_hasCreator_Person.csv
bash sort-convert-edge.sh ${dynamic_dir}/Comment_hasTag_Tag.csv
bash sort-convert-edge.sh ${dynamic_dir}/Comment_isLocatedIn_Place.csv
bash sort-convert-edge.sh ${dynamic_dir}/Comment_replyOf_Comment.csv
bash sort-convert-edge.sh ${dynamic_dir}/Comment_replyOf_Post.csv
bash sort-convert-edge.sh ${dynamic_dir}/Forum_containerOf_Post.csv
bash sort-convert-edge.sh ${dynamic_dir}/Forum_hasMember_Person.csv
bash sort-convert-edge.sh ${dynamic_dir}/Forum_hasModerator_Person.csv
bash sort-convert-edge.sh ${dynamic_dir}/Forum_hasTag_Tag.csv
bash sort-convert-edge.sh ${dynamic_dir}/Person_hasInterest_Tag.csv
bash sort-convert-edge.sh ${dynamic_dir}/Person_isLocatedIn_Place.csv
bash sort-convert-edge.sh ${dynamic_dir}/Person_knows_Person.csv
bash sort-convert-edge.sh ${dynamic_dir}/Person_likes_Comment.csv
bash sort-convert-edge.sh ${dynamic_dir}/Person_likes_Post.csv
bash sort-convert-edge.sh ${dynamic_dir}/Person_studyAt_Organisation.csv
bash sort-convert-edge.sh ${dynamic_dir}/Person_workAt_Organisation.csv
bash sort-convert-edge.sh ${dynamic_dir}/Post_hasCreator_Person.csv
bash sort-convert-edge.sh ${dynamic_dir}/Post_hasTag_Tag.csv
bash sort-convert-edge.sh ${dynamic_dir}/Post_isLocatedIn_Place.csv

echo "Static Data Processing..."

# Inplace sort node data
cd ${current_dir}
bash sort-convert-node.sh ${static_dir}/Organisation.csv
bash sort-convert-node.sh ${static_dir}/Place.csv
bash sort-convert-node.sh ${static_dir}/Tag.csv
bash sort-convert-node.sh ${static_dir}/TagClass.csv

# In place sort fwd edge data
bash sort-convert-edge.sh ${static_dir}/Organisation_isLocatedIn_Place.csv
bash sort-convert-edge.sh ${static_dir}/Place_isPartOf_Place.csv
bash sort-convert-edge.sh ${static_dir}/TagClass_isSubclassOf_TagClass.csv
bash sort-convert-edge.sh ${static_dir}/Tag_hasType_TagClass.csv

# echo "Person Knows Person FWD BWD Merging..."

cd ${current_dir}; python3 merge-fwd-bwd-edges.py ${dynamic_dir}
