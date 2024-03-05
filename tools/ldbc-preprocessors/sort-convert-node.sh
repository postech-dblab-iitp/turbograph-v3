#!/bin/bash

# Check if a file path is provided
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <csv_file>"
    exit 1
fi

file=$1

# Check if the file exists
if [ ! -f "$file" ]; then
    echo "File not found!"
    exit 1
fi

# Create a backup of the original file
cp $file $file.bak

# Read and store the header
header=$(head -n 1 $file)

# Check and replace the column name
new_header=$(echo $header | sed 's/:LABEL/label:STRING/')

# Sort the file numerically by the first column, excluding the header
# -n for numerical sort, -t, to specify comma as a delimiter, and -k1,1 to sort by the first column
tail -n +2 $file | sort -t, -k1,1n > /tmp/sorted_content.csv

# Combine the new header and the sorted content
echo $new_header > $file
cat /tmp/sorted_content.csv >> $file

# Clean up
rm /tmp/sorted_content.csv
rm $file.bak