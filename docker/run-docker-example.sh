#!/bin/bash

# TODO change
docker run -itd \
	--user "$(id -u):$(id -g)" \
	-v /home/jhko/dev/turbograph-v3:/turbograph-v3 \
	--shm-size=100g \
	--name tbgppv3-docker \
	tbgpp-v3:test