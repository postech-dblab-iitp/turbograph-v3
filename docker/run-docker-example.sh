#!/bin/bash

# Example usage: ./run-docker-example.sh /mnt/md0/jhha/data /mnt/md0/jhha/source-data

# Parse user input

# Target image
IMAGE_NAME="turbograph-image"
IMAGE_TAG="latest"
CONTAINER_NAME="turbograph-s62"

# TODO override from user input
SHARED_MEM_SIZE="450g"
# TODO add ulimit

CONTAINER_UID=$(id -u)
CONTAINER_GID=$(id -g)
CONTAINER_USERNAME="$(whoami)"
PROJECT_DIR=$( dirname $( readlink -f $( dirname -- "$0" ) ) )
[[ -z "$1" ]] && { echo "Provide DATA_DIR where data will be stored to!!!"; exit 1;}
DATA_DIR=$1
[[ -z "$2" ]] && { echo "Provide SOURCE_DATA_DIR where you load input data!!!"; exit 1;}
SOURCE_DATA_DIR=$2

# TODO you need to set /etc/passwd thus make another user, to access vscode
# TODO and then mkdir /home/USERNAME and chown
# TODO set entrypoints refer to commercial dbmss 
	# e.g. https://github.com/docker-library/postgres/blob/master/12/bullseye/docker-entrypoint.sh

	#--user "${CONTAINER_USERNAME}:${CONTAINER_GID}" \
docker run -itd --cap-add SYS_ADMIN \
	--cap-add SYS_PTRACE \
	--ulimit nofile=2000000000:2000000000	\
	-v ${PROJECT_DIR}:/turbograph-v3 \
	-v ${DATA_DIR}:/data \
	-v ${SOURCE_DATA_DIR}:/source-data \
	-v /mnt:/mnt \
	--shm-size=${SHARED_MEM_SIZE} \
	--entrypoint="/bin/bash" \
	--name ${CONTAINER_NAME} \
	${IMAGE_NAME}:${IMAGE_TAG} 