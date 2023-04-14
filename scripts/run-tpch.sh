#!/bin/bash

SUFFIX=""
SF=$1

./tbgpp-execution-engine/TurboGraph \
	--workspace:"/data/tpch/sf${SF}/"
