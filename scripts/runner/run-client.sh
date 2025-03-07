#!/bin/bash

BUILD_DIR="/turbograph-v3/build/tools/"
db_dir=$1


${BUILD_DIR}/client \
	--log-level info \
	--standalone \
	--workspace ${db_dir} \
	--log-level debug \
	--standalone \
	--disable-merge-join \
	--join-order-optimizer exhaustive \
	--profile \
	--explain 