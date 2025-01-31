if(POLICY CMP0063)
    cmake_policy(SET CMP0063 NEW)
endif()

add_library(duckdb_utf8proc STATIC utf8proc.cpp utf8proc_wrapper.cpp)

target_include_directories(
        duckdb_utf8proc
        PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

set_target_properties(duckdb_utf8proc PROPERTIES EXPORT_NAME duckdb_utf8proc)