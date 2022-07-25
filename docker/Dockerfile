# Dockerfile for turbograph-v3

FROM ubuntu:18.04
# TODO Fix
WORKDIR /root/

# Versions
ENV LLVM_RELEASE=14
ENV CMAKE_VERSION=3.16.0-0kitware1
ENV GCC_VERSION=9
	# TODO Add more

# Pre-configuration
RUN apt-get -qq update && \
	apt-get install -qqy --no-install-recommends software-properties-common lsb-release ca-certificates autoconf automake dpkg-dev file git make patch libc-dev libc++-dev dirmngr gnupg lbzip2 wget xz-utils libtinfo5 curl
RUN curl -fsSL https://bazel.build/bazel-release.pub.gpg | gpg --dearmor >bazel-archive-keyring.gpg \
	&& mv bazel-archive-keyring.gpg /usr/share/keyrings \
	&& echo "deb [arch=amd64 signed-by=/usr/share/keyrings/bazel-archive-keyring.gpg] https://storage.googleapis.com/bazel-apt stable jdk1.8" | tee /etc/apt/sources.list.d/bazel.list
RUN wget -O - https://apt.kitware.com/keys/kitware-archive-latest.asc 2>/dev/null | gpg --dearmor - | tee /usr/share/keyrings/kitware-archive-keyring.gpg >/dev/null && \
	echo 'deb [signed-by=/usr/share/keyrings/kitware-archive-keyring.gpg] https://apt.kitware.com/ubuntu/ bionic main' | tee /etc/apt/sources.list.d/kitware.list >/dev/null
	# for gcc version update
RUN add-apt-repository ppa:ubuntu-toolchain-r/test \
	&& apt-get -qq update \
	&& apt-get -qq --fix-broken install \
	&& apt-get -y remove gcc gcc-7
# RUN apt-get -qq update

# Build-related
RUN apt-get install -y \
		gcc-${GCC_VERSION} g++-${GCC_VERSION} \
		bazel \
		cmake=${CMAKE_VERSION} \
		cmake-data=${CMAKE_VERSION} \
		ninja-build \
	&& update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-9 50 --slave /usr/bin/g++ g++ /usr/bin/g++-9 \
	&& git clone https://github.com/catchorg/Catch2.git && cd Catch2 && cmake -Bbuild -H. -DBUILD_TESTING=OFF && cmake --build build/ --target install
	
# LLVM-related
# https://apt.llvm.org/
RUN wget https://apt.llvm.org/llvm.sh && chmod +x llvm.sh && ./llvm.sh ${LLVM_RELEASE}

# Essential libraries
RUN apt-get install -y \
		libtbb-dev \
		libaio-dev \
		numactl \
		libboost-all-dev

# TODO huh?
RUN git config --global --add safe.directory /turbograph-v3

# WORKDIR to project repository
WORKDIR /turbograph-v3/