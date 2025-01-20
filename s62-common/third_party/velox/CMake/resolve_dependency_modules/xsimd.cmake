# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
include_guard(GLOBAL)

set(VELOX_XSIMD_VERSION 10.0.1)
set(VELOX_XSIMD_BUILD_SHA256_CHECKSUM
d9535150dfd90e836045f013f89c616da4172ff31b727327b3d0e52ca06bf11d)
set(VELOX_XSIMD_SOURCE_URL
    "https://github.com/postech-dblab-iitp/xsimd/archive/refs/tags/${VELOX_XSIMD_VERSION}.tar.gz"
)

resolve_dependency_url(XSIMD)

message(STATUS "Building xsimd from source")
FetchContent_Declare(
  xsimd
  URL ${VELOX_XSIMD_SOURCE_URL}
  URL_HASH ${VELOX_XSIMD_BUILD_SHA256_CHECKSUM})

FetchContent_MakeAvailable(xsimd)
