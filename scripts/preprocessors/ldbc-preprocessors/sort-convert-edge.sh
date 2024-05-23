#!/bin/bash

# Check if a file path is provided
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <file_path>"
    exit 1
fi

file=$1
dynamic_dir=$(dirname "$file")
filename=$(basename "$file")

# Check if the file exists
if [ ! -f "$file" ]; then
    echo "File not found!"
    exit 1
fi

# Store the header and prepare the backward header
header=$(head -n 1 $file)
backward_header=$(echo $header | awk -F '|' '{print $2 "|" $1}')

# Sort the file by the first and second column, skipping the header
tail -n +2 $file | sort -t '|' -n -k 1,1 -k 2,2 > ${dynamic_dir}/temp_${filename}
# Re-add the header
(echo $header; cat ${dynamic_dir}/temp_${filename}) > ${file}

# Create and sort the backward file
# Flipping the columns and sorting by the new first (originally second) and then the new second (originally first)
echo $backward_header > ${dynamic_dir}/${filename}.backward
tail -n +2 $file | awk -F '|' -v OFS='|' '{print $2, $1}' | sort -t '|' -n -k 1,1 -k 2,2 >> ${dynamic_dir}/${filename}.backward

# Clean up
rm ${dynamic_dir}/temp_${filename}
