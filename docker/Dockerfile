# Dockerfile for turbograph-v3

FROM ubuntu:18.04
# TODO Fix
WORKDIR /root/

# Versions
ENV LLVM_RELEASE=14
	# TODO Add more

# Pre-configuration
RUN apt-get -qq update && \
	apt-get install -qqy --no-install-recommends software-properties-common lsb-release ca-certificates autoconf automake cmake dpkg-dev file git make patch libc-dev libc++-dev dirmngr gnupg lbzip2 wget xz-utils libtinfo5 curl
RUN curl -fsSL https://bazel.build/bazel-release.pub.gpg | gpg --dearmor >bazel-archive-keyring.gpg \
	&& mv bazel-archive-keyring.gpg /usr/share/keyrings \
	&& echo "deb [arch=amd64 signed-by=/usr/share/keyrings/bazel-archive-keyring.gpg] https://storage.googleapis.com/bazel-apt stable jdk1.8" | tee /etc/apt/sources.list.d/bazel.list
	# for gcc version update
RUN add-apt-repository ppa:ubuntu-toolchain-r/test && apt-get -qq update && apt-get -qq --fix-broken install
# RUN apt-get -qq update

# Build-related
RUN apt-get install -y \
		bazel \
		cmake \
		ninja-build

RUN git clone https://github.com/catchorg/Catch2.git && cd Catch2 && cmake -Bbuild -H. -DBUILD_TESTING=OFF && cmake --build build/ --target install

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