if(POLICY CMP0063)
    cmake_policy(SET CMP0063 NEW)
endif()

add_library(s62_simdjson STATIC s62_simdjson.cpp)

target_include_directories(
        s62_simdjson
        PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
set_target_properties(s62_simdjson PROPERTIES EXPORT_NAME s62_simdjson)