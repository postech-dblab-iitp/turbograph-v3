#!/bin/bash

SUFFIX=""
SF=$1

export SF=${SF}
./tbgpp-execution-engine/TurboGraph \
	--workspace:"/data/coco_demo/"
