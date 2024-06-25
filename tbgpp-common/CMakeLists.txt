cmake_minimum_required(VERSION 3.16)

####################################################
# PROJECT METADATA 
####################################################
project(tbgpp-common)

####################################################
# ADD SUBDIRECTORIES
####################################################
add_subdirectory(third_party)
#add_subdirectory(src/common/vector_operations)

####################################################
# COMPILATION FLAGS
####################################################
set(CMAKE_CXX_STANDARD 17)
if(NOT CMAKE_BUILD_TYPE)
	set(CMAKE_BUILD_TYPE Release)
endif()
set(CMAKE_POSITION_INDEPENDENT_CODE ON)


####################################################
# FIND PACKAGES
####################################################
find_package(Python3 COMPONENTS Interpreter Development REQUIRED)

####################################################
# DECLARE TARGETS
####################################################
add_library(tbgpp-common-lib STATIC)

####################################################
# GLOBAL DEFINITION
####################################################

####################################################
# PER-TARGET DEFINITION - tbgpp-common-lib
####################################################
target_include_directories(tbgpp-common-lib PUBLIC 
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
	# third_party
	"${PROJECT_SOURCE_DIR}/include/third_party"
	"${PROJECT_SOURCE_DIR}/include/third_party/duckdb_fmt/include"
	"${PROJECT_SOURCE_DIR}/include/third_party/duckdb_re2/"
	"${PROJECT_SOURCE_DIR}/include/third_party/fast_float"
	"${PROJECT_SOURCE_DIR}/include/third_party/utf8proc/include"
	"${PROJECT_SOURCE_DIR}/third_party/fastpforlib"
	"${PROJECT_SOURCE_DIR}/third_party/simdcsv"
	"${PROJECT_SOURCE_DIR}/third_party/icecream-cpp/include/"
	"${PROJECT_SOURCE_DIR}/third_party/tblr"
	"${PROJECT_SOURCE_DIR}/third_party/cuckoofilter/src/"
	"${PROJECT_SOURCE_DIR}/third_party/velox"
	"${PROJECT_SOURCE_DIR}/third_party/range-v3"
	"${PROJECT_SOURCE_DIR}/third_party/fplus"
	"${PROJECT_SOURCE_DIR}/include/third_party/simdjson"
	"${PROJECT_SOURCE_DIR}/include/third_party/fpgrowth/FP-growth"
	# external - client
	"${PROJECT_SOURCE_DIR}/../tbgpp-client/include/"
	"${PROJECT_SOURCE_DIR}/../tbgpp-execution-engine/include/"
	"${PROJECT_SOURCE_DIR}/../tbgpp-graph-store/include/"
	# python
	${Python3_INCLUDE_DIRS}
)
set(SRC_DIR "${PROJECT_SOURCE_DIR}/src")
file (GLOB_RECURSE SYSTEM_SOURCES "${SRC_DIR}/*.c*"
	# third_party
	"${PROJECT_SOURCE_DIR}/third_party/cuckoofilter/src/*.c*")

SET(TBGPP_SYSTEM_SOURCES ${SYSTEM_SOURCES})
target_sources(tbgpp-common-lib PUBLIC ${TBGPP_SYSTEM_SOURCES})
target_link_libraries(tbgpp-common-lib pthread aio numa rt hwloc tbb ssl crypto readline)
target_link_libraries(tbgpp-common-lib duckdb_fastpforlib)
target_link_libraries(tbgpp-common-lib turbograph_simdcsv)
target_link_libraries(tbgpp-common-lib
	velox_functions_prestosql
	velox_functions_prestosql_impl
	velox_aggregates
	velox_vector_test_lib
	velox_exec
	velox_link_libs
	velox_dwio_dwrf_reader
	velox_dwio_dwrf_writer)
target_link_libraries(tbgpp-common-lib ${Python3_LIBRARIES})
target_compile_options(tbgpp-common-lib PUBLIC ${CXX_FLAGS})
target_compile_definitions(tbgpp-common-lib PUBLIC ${CXX_DEFINITION})

####################################################
# TESTS
####################################################
