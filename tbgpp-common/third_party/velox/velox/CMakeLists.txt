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

# VELOX BASE
include_directories(external/xxhash)
add_subdirectory(buffer)
add_subdirectory(common)
add_subdirectory(type)
add_subdirectory(vector)
add_subdirectory(flag_definitions)
add_subdirectory(external/date)

# hive connector depends on dwio
if(${VELOX_ENABLE_HIVE_CONNECTOR})
  add_subdirectory(dwio)
endif()

# add_subdirectory(functions) # depends on md5 (postgresql)
# add_subdirectory(connectors)

# # exec depends on codegen
# if(${VELOX_ENABLE_EXEC})
#   add_subdirectory(exec)
# endif()