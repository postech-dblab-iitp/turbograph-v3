set(CMAKE_C_VISIBILITY_PRESET hidden)

set(VELOX_BUILD_MINIMAL_WITH_DWIO ON CACHE BOOL "Build only DWIO components.")
set(VELOX_BUILD_TESTING OFF CACHE BOOL "Build tests.")
set(VELOX_BUILD_MINIMAL OFF CACHE BOOL "Build only minimal components.")
set(JSON_BuildTests OFF CACHE INTERNAL "")

# Note: header only libraries are excluded
add_subdirectory(antlr4_cypher)
add_subdirectory(antlr4_runtime)
add_subdirectory(cuckoofilter)
add_subdirectory(duckdb_fmt)
add_subdirectory(duckdb_re2)
add_subdirectory(fastpforlib)
add_subdirectory(json)
add_subdirectory(simdcsv)
add_subdirectory(utf8proc)
add_subdirectory(velox)
add_subdirectory(yyjson)