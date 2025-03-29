#!/bin/bash

BUILD_DIR="/turbograph-v3/build-release/tools/"
db_dir=$1


${BUILD_DIR}/client \
	--log-level info \
	--standalone \
	--workspace ${db_dir} \
	--disable-merge-join \
	--join-order-optimizer exhaustive \
	--profile \
	--explain \