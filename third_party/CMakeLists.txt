set(CMAKE_C_VISIBILITY_PRESET hidden)

set(VELOX_BUILD_MINIMAL_WITH_DWIO ON CACHE BOOL "Build Velox minimally with DWIO support")

add_subdirectory(velox)