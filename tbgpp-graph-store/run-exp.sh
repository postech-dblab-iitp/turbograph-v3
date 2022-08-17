#!/bin/bash

SCALE=100

dynamic_vertices=(Post)
#dynamic_vertices=(Person Forum Post Comment)
static_vertices=(Organisation Place Tag TagClass)

for i in ${dynamic_vertices[@]}; do
	# Drop Page Caches
	#echo 3 | tee /proc/sys/vm/drop_caches
	# Remove Existing DB
	rm -rf /data/data/*
	# Run exp
	build/extent_test_ldbc --nodes:${i} /source-data/sf${SCALE}/graphs/csv/interactive/composite-projected-fk/dynamic/${i}.csv > ${i}_Bulkloading.txt
done

for i in ${static_vertices[@]}; do
	continue
	# Drop Page Caches
	#echo 3 | sudo tee /proc/sys/vm/drop_caches
	# Remove Existing DB
	rm -rf /data/data/*
	# Run exp
	build/extent_test_ldbc --nodes:${i} /data/interactive/sf${SCALE}/graphs/csv/interactive/composite-projected-fk/static/${i}.csv > ${i}_Bulkloading.txt
done
