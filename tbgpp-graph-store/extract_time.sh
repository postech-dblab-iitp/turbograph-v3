#!/bin/bash

file=$1

init_csv=`(grep -nr -e 'InitCSV' $file | awk '{s+=$3}END{print s}')`
read_csv=`(grep -nr -e 'Read CSV File' $file | awk '{s+=$7}END{print s}')`
create_ext=`(grep -nr -e 'CreateExtent' $file | awk '{s+=$4}END{print s}')`
build_map=`(grep -nr -e 'Map Build' $file | awk '{s+=$4}END{print s}')`

total_elapsed=`(grep -nr -e 'Load' $file | grep -e 'Done!' | grep -e 'Elapsed' | awk '{s+=$6}END{print s}')`

echo "INIT_CSV" $init_csv "READ_CSV" $read_csv "CREATE_EXT" $create_ext, "BUILD_MAP" $build_map, "TOTAL" $total_elapsed
