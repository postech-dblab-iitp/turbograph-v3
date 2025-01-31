if(POLICY CMP0063)
    cmake_policy(SET CMP0063 NEW)
endif()

add_library(duckdb_fmt STATIC format.cc)

target_include_directories(duckdb_fmt PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
set_target_properties(duckdb_fmt PROPERTIES EXPORT_NAME duckdb_fmt)