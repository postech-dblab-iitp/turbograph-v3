set(CMAKE_C_VISIBILITY_PRESET hidden)

set(VELOX_BUILD_MINIMAL_WITH_DWIO ON CACHE BOOL "Build only DWIO components.")
set(VELOX_BUILD_TESTING OFF CACHE BOOL "Build tests.")
set(VELOX_BUILD_MINIMAL OFF CACHE BOOL "Build only minimal components.")

message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")

add_subdirectory(velox)