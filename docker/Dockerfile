# Dockerfile for turbograph-v3

FROM ubuntu:20.04

# Set working directory
WORKDIR /root/

# Environment variables
ENV LLVM_RELEASE=14 \
    CMAKE_VERSION=3.28.6 \
    GCC_VERSION=11 \
    DEBIAN_FRONTEND=noninteractive

# Pre-configuration: essential packages, repositories, and keys
RUN apt-get -qq update && \
    apt-get install -qqy --no-install-recommends software-properties-common lsb-release ca-certificates autoconf automake dpkg-dev file git make patch libc-dev libc++-dev dirmngr gnupg lbzip2 wget xz-utils libtinfo5 curl
RUN curl -fsSL https://bazel.build/bazel-release.pub.gpg | gpg --dearmor >bazel-archive-keyring.gpg && \
    mv bazel-archive-keyring.gpg /usr/share/keyrings && \
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/bazel-archive-keyring.gpg] https://storage.googleapis.com/bazel-apt stable jdk1.8" | tee /etc/apt/sources.list.d/bazel.list
RUN wget -O- https://apt.repos.intel.com/intel-gpg-keys/GPG-PUB-KEY-INTEL-SW-PRODUCTS.PUB | gpg --dearmor | tee /usr/share/keyrings/oneapi-archive-keyring.gpg > /dev/null && \
    echo "deb [signed-by=/usr/share/keyrings/oneapi-archive-keyring.gpg] https://apt.repos.intel.com/oneapi all main" | tee /etc/apt/sources.list.d/oneAPI.list
RUN add-apt-repository ppa:ubuntu-toolchain-r/test && \
    apt-get -qq update && \
    apt-get -qq --fix-broken install && \
    apt-get -y remove gcc

# Build-related dependencies
RUN apt-get install -y \
    gcc-${GCC_VERSION} g++-${GCC_VERSION} \
    bazel \
    ninja-build \
    doxygen \
    libssl-dev \
    libtbb-dev \
    libaio-dev \
    numactl \
    libnuma-dev \
    vim \
    dstat \
    intel-basekit \
    gdb \
    sudo \
    libgoogle-perftools-dev \
    libreadline-dev \
    libhwloc-dev \
    openmpi-bin \
    libopenmpi-dev \
    python3-pip && \
    update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-${GCC_VERSION} 50 --slave /usr/bin/g++ g++ /usr/bin/g++-${GCC_VERSION}

# Install CMake
RUN curl -OL https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}.tar.gz && \
    tar -xzf cmake-${CMAKE_VERSION}.tar.gz && \
    cd cmake-${CMAKE_VERSION} && \
    ./bootstrap && \
    make -j$(nproc) && \
    make install

# Install LLVM
RUN wget https://apt.llvm.org/llvm.sh && chmod +x llvm.sh && ./llvm.sh ${LLVM_RELEASE}

# Install Catch2
RUN git clone https://github.com/catchorg/Catch2.git && \
    cd Catch2 && \
    cmake -Bbuild -H. -DBUILD_TESTING=OFF && \
    cmake --build build/ --target install

# install Xerces for Orca
RUN git clone https://github.com/greenplum-db/gp-xerces-archive.git gp-xerces && \
    cd gp-xerces && \
    mkdir build && cd build && \
    ../configure --prefix=/usr/local && \
    make && \
    make install

# Install SparseHash
RUN git clone https://github.com/sparsehash/sparsehash.git && \
    cd sparsehash && \
    ./configure && \
    make && \
    make install

# Python dependencies
RUN pip3 install --no-cache-dir scikit-learn

# Resolve git safe directory issue
RUN git config --global --add safe.directory /turbograph-v3

# WORKDIR to project repository
WORKDIR /turbograph-v3/

