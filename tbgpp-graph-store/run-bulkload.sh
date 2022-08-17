#!/bin/bash

SCALE=1

rm -rf /data/data/*
# Run exp
build/extent_test_ldbc \
	--nodes:Person /source-data/sf${SCALE}/graphs/csv/interactive/composite-projected-fk/dynamic/Person.csv \
	--nodes:Comment /source-data/sf${SCALE}/graphs/csv/interactive/composite-projected-fk/dynamic/Comment.csv \
	--relationships:LIKES /source-data/sf${SCALE}/graphs/csv/interactive/composite-projected-fk/dynamic/Person_likes_Comment.csv \
	--relationships_backward:LIKES /source-data/sf${SCALE}/graphs/csv/interactive/composite-projected-fk/dynamic/Person_likes_Comment.csv.backward
