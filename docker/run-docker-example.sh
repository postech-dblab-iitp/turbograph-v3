#!/bin/bash

# Parse user input

# Target image
IMAGE_NAME="tbgpp-v3"
IMAGE_TAG="test"
CONTAINER_NAME="tbgppv3-docker"

# TODO override from user input
SHARED_MEM_SIZE="100g"
# TODO add ulimit

DOCKER_UID=$(id -u)
DOCKER_GID=$(id -g)
PROJECT_DIR=$( dirname $( readlink -f $( dirname -- "$0" ) ) )
[[ -z "$1" ]] && { echo "Provide DATA_DIR where data will be stored to!!!"; exit 1;}
DATA_DIR=$1
[[ -z "$2" ]] && { echo "Provide SOURCE_DATA_DIR where you load input data!!!"; exit 1;}
SOURCE_DATA_DIR=$2

# TODO you need to set /etc/passwd thus make another user, to access vscode
# TODO set entrypoints refer to commercial dbmss 
	# e.g. https://github.com/docker-library/postgres/blob/master/12/bullseye/docker-entrypoint.sh

docker run -itd \
	--user "${DOCKER_UID}:${DOCKER_GID}" \
	-v ${PROJECT_DIR}:/turbograph-v3 \
	-v ${DATA_DIR}:/data \
	-v ${SOURCE_DATA_DIR}:/source-data \
	--shm-size=${SHARED_MEM_SIZE} \
	--name ${CONTAINER_NAME} \
	${IMAGE_NAME}:${IMAGE_TAG}
