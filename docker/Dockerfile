# Dockerfile for turbograph-v3

FROM ubuntu:20.04
# TODO Fix
WORKDIR /root/

# Versions
ENV LLVM_RELEASE=14
ENV CMAKE_VERSION=3.16.0-0kitware1
ENV GCC_VERSION=11
ENV BOOST_MAIN_VERSION=1
ENV BOOST_SUB_VERSION=78
	# TODO Add more

ARG DEBIAN_FRONTEND=noninteractive

# Pre-configuration
RUN apt-get -qq update && \
	apt-get install -qqy --no-install-recommends software-properties-common lsb-release ca-certificates autoconf automake dpkg-dev file git make patch libc-dev libc++-dev dirmngr gnupg lbzip2 wget xz-utils libtinfo5 curl
RUN curl -fsSL https://bazel.build/bazel-release.pub.gpg | gpg --dearmor >bazel-archive-keyring.gpg \
	&& mv bazel-archive-keyring.gpg /usr/share/keyrings \
	&& echo "deb [arch=amd64 signed-by=/usr/share/keyrings/bazel-archive-keyring.gpg] https://storage.googleapis.com/bazel-apt stable jdk1.8" | tee /etc/apt/sources.list.d/bazel.list
RUN wget -O - https://apt.kitware.com/keys/kitware-archive-latest.asc 2>/dev/null | gpg --dearmor - | tee /usr/share/keyrings/kitware-archive-keyring.gpg >/dev/null && \
	echo 'deb [signed-by=/usr/share/keyrings/kitware-archive-keyring.gpg] https://apt.kitware.com/ubuntu/ bionic main' | tee /etc/apt/sources.list.d/kitware.list >/dev/null
RUN wget -O- https://apt.repos.intel.com/intel-gpg-keys/GPG-PUB-KEY-INTEL-SW-PRODUCTS.PUB \
	| gpg --dearmor | tee /usr/share/keyrings/oneapi-archive-keyring.gpg > /dev/null \
	&& echo "deb [signed-by=/usr/share/keyrings/oneapi-archive-keyring.gpg] https://apt.repos.intel.com/oneapi all main" | tee /etc/apt/sources.list.d/oneAPI.list
# for gcc version update
RUN add-apt-repository ppa:ubuntu-toolchain-r/test \
	&& apt-get -qq update \
	&& apt-get -qq --fix-broken install \
	&& apt-get -y remove gcc gcc-7
# RUN apt-get -qq update

# Build-related
#RUN apt-get install -y \
#		gcc-${GCC_VERSION} g++-${GCC_VERSION} \
#		bazel \
#		cmake=${CMAKE_VERSION} \
#		cmake-data=${CMAKE_VERSION} \
#		ninja-build \
#		doxygen \
#	&& update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-${GCC_VERSION} 50 --slave /usr/bin/g++ g++ /usr/bin/g++-${GCC_VERSION} \
#	&& git clone https://github.com/catchorg/Catch2.git && cd Catch2 && cmake -Bbuild -H. -DBUILD_TESTING=OFF && cmake --build build/ --target install
	
# LLVM-related
# https://apt.llvm.org/
RUN wget https://apt.llvm.org/llvm.sh && chmod +x llvm.sh && ./llvm.sh ${LLVM_RELEASE}

# Essential libraries
RUN apt-get install -y \
		libtbb-dev \
		libaio-dev \
		numactl \
		libnuma-dev

# (boost)
#RUN wget http://downloads.sourceforge.net/project/boost/boost/${BOOST_MAIN_VERSION}.${BOOST_SUB_VERSION}.0/boost_${BOOST_MAIN_VERSION}_${BOOST_SUB_VERSION}_0.tar.gz \
#  && tar xfz boost_${BOOST_MAIN_VERSION}_${BOOST_SUB_VERSION}_0.tar.gz \
#  && rm boost_${BOOST_MAIN_VERSION}_${BOOST_SUB_VERSION}_0.tar.gz \
#  && cd boost_${BOOST_MAIN_VERSION}_${BOOST_SUB_VERSION}_0 \
#  && ./bootstrap.sh --prefix=/usr/local --with-libraries=program_options
#  && ./b2 install \
#  && cd .. \
#  && rm -rf boost_${BOOST_MAIN_VERSION}_${BOOST_SUB_VERSION}_0

# TODO need to install xerces for installing orca

#### For developers # TODO further separate
RUN apt-get install -y \
	vim \
	dstat \
	intel-basekit \
	gdb

# Resolve git issue
RUN git config --global --add safe.directory /turbograph-v3

# WORKDIR to project repository
WORKDIR /turbograph-v3/
