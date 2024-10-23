#!/bin/bash
db_dir=$1
data_dir=$2
distribution="zipf_0"
/turbograph-v3/build-release/tbgpp-execution-engine/bulkload_using_map \
    --output_dir:${db_dir} \
    --nodes:Package ${data_dir}/Package.csv \
    --jsonl:"--file_path:${data_dir}/Object_${distribution}.json --nodes:Object" \
    --nodes:Field ${data_dir}/Field.csv \
    --relationships:OBJECT_CAN_TRANSFORM_INTO_OBJECT ${data_dir}/Object_CAN_TRANSFORM_INTO_Object.csv \
    --relationships_backward:OBJECT_CAN_TRANSFORM_INTO_OBJECT ${data_dir}/Object_CAN_TRANSFORM_INTO_Object.csv.backward \
    --relationships:PACKAGE_CAN_TRANSFORM_INTO_PACKAGE ${data_dir}/Package_CAN_TRANSFORM_INTO_Package.csv \
    --relationships_backward:PACKAGE_CAN_TRANSFORM_INTO_PACKAGE ${data_dir}/Package_CAN_TRANSFORM_INTO_Package.csv.backward \
    --relationships:FIELD_CAN_TRANSFORM_INTO_FIELD ${data_dir}/Field_CAN_TRANSFORM_INTO_Field.csv \
    --relationships_backward:FIELD_CAN_TRANSFORM_INTO_FIELD ${data_dir}/Field_CAN_TRANSFORM_INTO_Field.csv.backward \
    --relationships:OBJECT_COMPOSED_OF_FIELD ${data_dir}/Object_COMPOSED_OF_Field.csv \
    --relationships_backward:OBJECT_COMPOSED_OF_FIELD ${data_dir}/Object_COMPOSED_OF_Field.csv.backward