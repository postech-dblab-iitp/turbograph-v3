cmake_minimum_required(VERSION 3.16)

####################################################
# PROJECT METADATA 
####################################################
project(common)

####################################################
# DECLARE TARGETS
####################################################
add_library(common-lib STATIC)

####################################################
# GLOBAL DEFINITION
####################################################

####################################################
# PER-TARGET DEFINITION - common-lib
####################################################
set(SRC_DIR "${PROJECT_SOURCE_DIR}/")
file (GLOB_RECURSE SYSTEM_SOURCES "${SRC_DIR}/*.c*")

set(FUNC_DIR "${PROJECT_SOURCE_DIR}/../function") 
file(GLOB_RECURSE FUNCTION_SOURCES "${FUNC_DIR}/*.cpp") 

set(PARSER_DIR "${PROJECT_SOURCE_DIR}/../parser") 
file(GLOB_RECURSE PARSER_SOURCES "${PARSER_DIR}/*.cpp") 

set(PLANNER_DIR "${PROJECT_SOURCE_DIR}/../planner") 
file(GLOB_RECURSE PLANNER_SOURCES "${PLANNER_DIR}/*.cpp") 

SET(TBGPP_SYSTEM_SOURCES ${SYSTEM_SOURCES} ${FUNCTION_SOURCES} ${PARSER_SOURCES} ${PLANNER_SOURCES})
target_sources(common-lib PUBLIC ${TBGPP_SYSTEM_SOURCES})
target_link_libraries(common-lib pthread aio numa rt hwloc tbb ssl crypto readline)
target_link_libraries(common-lib duckdb_fastpforlib)
target_link_libraries(common-lib turbograph_simdcsv)
target_link_libraries(common-lib cuckoofilter)
target_link_libraries(common-lib duckdb_fmt)
target_link_libraries(common-lib duckdb_re2)
target_link_libraries(common-lib s62_simdjson)
target_link_libraries(common-lib duckdb_utf8proc)
target_link_libraries(common-lib duckdb_yyjson)
target_link_libraries(common-lib ${Python3_LIBRARIES})
target_compile_options(common-lib PUBLIC ${CXX_FLAGS})
target_compile_definitions(common-lib PUBLIC ${CXX_DEFINITION})

####################################################
# TESTS
####################################################
