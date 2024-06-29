#!/bin/bash

db_dir=$1
./tbgpp-client/TurboGraph-S62 \
	--workspace:${db_dir} \
	--disable-merge-join \
	--join-order-optimizer:exhaustive