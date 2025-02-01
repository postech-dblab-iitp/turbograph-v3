if(POLICY CMP0063)
    cmake_policy(SET CMP0063 NEW)
endif()

add_library(duckdb_yyjson STATIC yyjson.c)

target_include_directories(
        duckdb_yyjson
        PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

set_target_properties(duckdb_yyjson PROPERTIES EXPORT_NAME duckdb_yyjson)