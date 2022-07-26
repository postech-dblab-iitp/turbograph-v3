#!/bin/bash

# TODO change
docker run -itd \
	--pid=host --cap-add=SYS_ADMIN --cap-add=SYS_PTRACE \
	--security-opt seccomp:unconfined \
	--user "$(id -u):$(id -g)" \
	-v $PWD/../:/turbograph-v3 \
	-v /mnt/md0/tslee_ldbc:/data \
	--shm-size=100g \
	--name tbgppv3-docker2 \
	tbgpp-v3:test
