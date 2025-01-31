cmake_minimum_required(VERSION 3.16)

####################################################
# PROJECT METADATA 
####################################################
project(common)

####################################################
# FIND PACKAGES
####################################################
find_package(Python3 COMPONENTS Interpreter Development REQUIRED)

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
target_include_directories(common-lib PUBLIC 
	# include
	"${PROJECT_SOURCE_DIR}/include"
	# common
	"${PROJECT_SOURCE_DIR}/include/common"
	"${PROJECT_SOURCE_DIR}/include/common/enums"
	"${PROJECT_SOURCE_DIR}/include/common/types"
	"${PROJECT_SOURCE_DIR}/include/common/operator"
	"${PROJECT_SOURCE_DIR}/include/common/value_operations"
	"${PROJECT_SOURCE_DIR}/include/common/vector_operations"
	# parser
	"${PROJECT_SOURCE_DIR}/include/parser"
	"${PROJECT_SOURCE_DIR}/include/parser/parsed_data"
	"${PROJECT_SOURCE_DIR}/include/parser/constraints"
	"${PROJECT_SOURCE_DIR}/include/parser/expression"
	"${PROJECT_SOURCE_DIR}/include/parser/query_node"
	"${PROJECT_SOURCE_DIR}/include/parser/statement"
	"${PROJECT_SOURCE_DIR}/include/parser/tableref"
	# planner
	"${PROJECT_SOURCE_DIR}/include/planner"
	"${PROJECT_SOURCE_DIR}/include/planner/constraints"
	"${PROJECT_SOURCE_DIR}/include/planner/expression"
	"${PROJECT_SOURCE_DIR}/include/planner/expression_binder"
	"${PROJECT_SOURCE_DIR}/include/planner/filter"
	"${PROJECT_SOURCE_DIR}/include/planner/operator"
	"${PROJECT_SOURCE_DIR}/include/planner/parsed_data"
	"${PROJECT_SOURCE_DIR}/include/planner/query_node"
	"${PROJECT_SOURCE_DIR}/include/planner/subquery"
	"${PROJECT_SOURCE_DIR}/include/planner/tableref"
	# external - client
	"${PROJECT_SOURCE_DIR}/../client/include/"
	"${PROJECT_SOURCE_DIR}/../execution/include/"
	# python
	${Python3_INCLUDE_DIRS}
)
set(SRC_DIR "${PROJECT_SOURCE_DIR}/src")
file (GLOB_RECURSE SYSTEM_SOURCES "${SRC_DIR}/*.c*")

set(FUNC_DIR "${PROJECT_SOURCE_DIR}/../function") 
file(GLOB_RECURSE FUNCTION_SOURCES "${FUNC_DIR}/*.cpp") 

SET(TBGPP_SYSTEM_SOURCES ${SYSTEM_SOURCES} ${FUNCTION_SOURCES})
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
