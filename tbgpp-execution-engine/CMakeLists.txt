cmake_minimum_required(VERSION 3.16)

###################################################
# PROJECT METADATA
###################################################
project(tbgpp-execution-engine)

####################################################
# ADD SUBDIRECTORIES
####################################################
# TODO replace with boost::json and eliminate
# dependencies - json
set(JSON_BuildTests OFF CACHE INTERNAL "")
add_subdirectory(json)

####################################################
# COMPILATION FLAGS
####################################################
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_CXX_STANDARD 17)

####################################################
# FIND PACKAGES
####################################################
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake")

# dependencies - boost
#set(Boost_DEBUG 1)
include(cmake/FindBoost.cmake)
find_package(Boost REQUIRED)
include_directories(${Boost_INCLUDE_DIRS})
link_directories(${Boost_LIBRARY_DIRS})
link_libraries(${Boost_LIBRARIES})

#dependencies - threads
find_package(Threads REQUIRED)
link_libraries(Threads::Threads)
include_directories(${TBB_INCLUDE_DIRS})
link_directories(${TBB_LIBRARY_DIRS})
link_libraries(${TBB_LIBRARIES})

# dependencies - openmp
find_package(OpenMP REQUIRED)
set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS}")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${OpenMP_EXE_LINKER_FLAGS}")

# dependencies - livegraph
#add_library(livegraph SHARED IMPORTED)
#set_property(TARGET livegraph PROPERTY IMPORTED_LOCATION "${PROJECT_SOURCE_DIR}/livegraph/liblivegraph.so")
#target_include_directories(livegraph INTERFACE "${PROJECT_SOURCE_DIR}/livegraph/bind")
#target_include_directories(livegraph INTERFACE "${PROJECT_SOURCE_DIR}/livegraph/core")
#include_directories("${PROJECT_SOURCE_DIR}/livegraph/core")

####################################################
# DECLARE TARGETS
####################################################
add_library(tbgpp-execution-engine-lib STATIC)

####################################################
# GLOBAL DEFINITION
####################################################
add_definitions(-DDUCKDB_SOURCE_ID="\""1"\"")
add_definitions(-DDUCKDB_VERSION="\""1"\"")

####################################################
# PER-TARGET DEFINITION - tbgpp-execution-engine-lib
####################################################
set_target_properties(tbgpp-execution-engine-lib PROPERTIES LINKER_LANGUAGE CXX)
set(SRC_DIR "${PROJECT_SOURCE_DIR}/src")
file (GLOB_RECURSE SOURCES "${SRC_DIR}/*.c*" 
	# for debugging purpose, temporary
	#"${PROJECT_SOURCE_DIR}/test/plans/ldbc_snb_interactive/*.c*"
	#"${PROJECT_SOURCE_DIR}/test/plans/*.c*"
)
target_include_directories( tbgpp-execution-engine-lib PUBLIC "${PROJECT_SOURCE_DIR}/include" )
target_include_directories( tbgpp-execution-engine-lib PUBLIC
	# tbgpp-common
  "${PROJECT_SOURCE_DIR}/../tbgpp-common/include"
	"${PROJECT_SOURCE_DIR}/../tbgpp-common/include/common"
  "${PROJECT_SOURCE_DIR}/../tbgpp-common/include/common/enums"
	"${PROJECT_SOURCE_DIR}/../tbgpp-common/include/common/types"
  "${PROJECT_SOURCE_DIR}/../tbgpp-common/include/common/operator"
	"${PROJECT_SOURCE_DIR}/../tbgpp-common/include/common/value_operations" 
  #"${PROJECT_SOURCE_DIR}/../tbgpp-common/include/common/vector_operations"
  # parser
	"${PROJECT_SOURCE_DIR}/../include/parser"
	"${PROJECT_SOURCE_DIR}/../include/parser/parsed_data"
  "${PROJECT_SOURCE_DIR}/include/parser/constraints"
	"${PROJECT_SOURCE_DIR}/include/parser/expression"
	"${PROJECT_SOURCE_DIR}/include/parser/query_node"
	"${PROJECT_SOURCE_DIR}/include/parser/statement"
	"${PROJECT_SOURCE_DIR}/include/parser/tableref"
  # tbgpp-client
  "${PROJECT_SOURCE_DIR}/../tbgpp-client/include/main"
)
target_sources( tbgpp-execution-engine-lib PUBLIC "${SOURCES}" )
target_link_libraries( tbgpp-execution-engine-lib
  tbgpp-common-lib
  tbgpp-main
  "-pthread -ldl -lboost_system -lboost_timer -lboost_date_time -lboost_filesystem"
)

####################################################
# TESTS
####################################################
file(GLOB ENGINE_TEST_SRC
  #  "${PROJECT_SOURCE_DIR}/test/TurboGraph.cpp"
  #  "${PROJECT_SOURCE_DIR}/test/bulkload_using_index.cpp"
  "${PROJECT_SOURCE_DIR}/test/bulkload_using_map.cpp"
  #  "${PROJECT_SOURCE_DIR}/test/bulkload_using_map2.cpp"
  #  "${PROJECT_SOURCE_DIR}/test/bulkload_json.cpp"
)
file (GLOB_RECURSE SOURCES "${SRC_DIR}/*.c*")
file (GLOB_RECURSE TEST_SOURCES
	#   "${PROJECT_SOURCE_DIR}/test/old_plans/simple/test12.cpp"
#   "${PROJECT_SOURCE_DIR}/test/plans/ldbc_snb_interactive/*.cpp"
#   "${PROJECT_SOURCE_DIR}/test/plans/tpc-h/*.cpp"
#   "${PROJECT_SOURCE_DIR}/test/plans/coco/*.cpp"
)

foreach(test_file ${ENGINE_TEST_SRC})
  get_filename_component(prog_name ${test_file} NAME_WE)
  add_executable(${prog_name} ${test_file} )
  target_sources( ${prog_name} PUBLIC ${SOURCES} )
  target_sources( ${prog_name} PUBLIC ${TEST_SOURCES}  ) 
  target_compile_options(${prog_name} PRIVATE ${CXX_FLAGS})
	target_compile_definitions(${prog_name} PRIVATE ${CXX_DEFINITION})
  target_include_directories( ${prog_name} PUBLIC
    # tbgpp-common
    "${PROJECT_SOURCE_DIR}/../tbgpp-common/include/common" "${PROJECT_SOURCE_DIR}/../tbgpp-common/include/common/enums"
    "${PROJECT_SOURCE_DIR}/../tbgpp-common/include/common/types" "${PROJECT_SOURCE_DIR}/../tbgpp-common/include/common/operator"
    "${PROJECT_SOURCE_DIR}/../tbgpp-common/include/common/value_operations" "${PROJECT_SOURCE_DIR}/../tbgpp-common/include/common/vector_operations"
    # test
    "${PROJECT_SOURCE_DIR}/test/"
  )
  target_link_libraries(${prog_name} PRIVATE
    tbgpp-main
    tbgpp-common-lib
    tbgpp-graph-store-lib
    tbgpp-execution-engine-lib
    nlohmann_json::nlohmann_json "-pthread -ldl -lboost_system -lboost_timer -lboost_date_time -lboost_filesystem"
  )
endforeach(test_file ${TILE_TEST_SRC})
