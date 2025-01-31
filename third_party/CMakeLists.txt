set(CMAKE_C_VISIBILITY_PRESET hidden)

set(VELOX_BUILD_MINIMAL_WITH_DWIO ON CACHE BOOL "Build only DWIO components.")
set(VELOX_BUILD_TESTING OFF CACHE BOOL "Build tests.")
set(VELOX_BUILD_MINIMAL OFF CACHE BOOL "Build only minimal components.")

# Note: header only libraries are excluded
add_subdirectory(velox)
add_subdirectory(antlr4_cypher)
add_subdirectory(antlr4_runtime)
add_subdirectory(fastpforlib)
add_subdirectory(simdcsv)
add_subdirectory(cuckoofilter)