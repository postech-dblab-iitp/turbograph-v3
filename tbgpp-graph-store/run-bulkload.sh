#!/bin/bash

SCALE=100

rm -rf /data/data/*
# Run exp
build/extent_test_ldbc \
	--nodes:Person /data/interactive/sf${SCALE}/graphs/csv/interactive/composite-projected-fk/dynamic/Person.csv \
	--nodes:Comment /data/interactive/sf${SCALE}/graphs/csv/interactive/composite-projected-fk/dynamic/Comment.csv \
	--relationships:LIKES /data/interactive/sf${SCALE}/graphs/csv/interactive/composite-projected-fk/dynamic/Person_likes_Comment.csv
