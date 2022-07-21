#!/bin/bash

SCALE=100

rm -rf /data/data/*
build/extent_test_ldbc --nodes:Post /data/interactive/sf${SCALE}/graphs/csv/interactive/composite-projected-fk/dynamic/Post.csv
