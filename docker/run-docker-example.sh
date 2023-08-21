#!/bin/bash

# Example usage: ./run-docker-example.sh /mnt/md0/stlee_ldbc/tbgpp3-data/ /mnt/md0/tslee_ldbc/interactive

# Parse user input

# Target image
IMAGE_NAME="tbgpp-v3-u20.04"
IMAGE_TAG="sys"
CONTAINER_NAME="tbgppv3-docker-tslee"

# TODO override from user input
SHARED_MEM_SIZE="100g"
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
	-v ${PROJECT_DIR}:/turbograph-v3 \
	-v ${DATA_DIR}:/data \
	-v ${SOURCE_DATA_DIR}:/source-data \
	--shm-size=${SHARED_MEM_SIZE} \
	--entrypoint="/bin/bash" \
	--name ${CONTAINER_NAME} \
	${IMAGE_NAME}:${IMAGE_TAG} \

# TODO fix this.
#	-c "groupadd -g ${CONTAINER_GID} ${CONTAINER_USERNAME}; useradd -u ${CONTAINER_UID} -g ${CONTAINER_GID} -ms /bin/bash ${CONTAINER_USERNAME}; tail -f /dev/null;":