#!/bin/bash

db_dir=$1
./tbgpp-execution-engine/TurboGraph \
	--workspace:${db_dir}
