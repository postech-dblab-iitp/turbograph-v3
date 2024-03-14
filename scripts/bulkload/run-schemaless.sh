#!/bin/bash

log_prefix=$1

for i in {38..28}; do
	for j in {1..16}; do
		begin_val=$(($RANDOM%101))
		end_val=$(($RANDOM%101))
		if [ $begin_val -gt $end_val ]; then
			tmp=$begin_val
			begin_val=$end_val
			end_val=$tmp
		fi
		../build_release/tbgpp-graph-store/schemaless_simulation "/source-data/schemaless/${i}_shuf.csv" 0 0 0 2 3 ${begin_val} ${end_val} >> ${log_prefix}_${i}_shuf.txt
	done
done
