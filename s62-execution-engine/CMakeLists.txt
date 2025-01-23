cmake_minimum_required(VERSION 3.16)

###################################################
# PROJECT METADATA
###################################################
project(s62-execution-engine)

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

####################################################
# DECLARE TARGETS
####################################################
add_library(s62-execution-engine-lib STATIC)

####################################################
# GLOBAL DEFINITION
####################################################
add_definitions(-DDUCKDB_SOURCE_ID="\""1"\"")
add_definitions(-DDUCKDB_VERSION="\""1"\"")

####################################################
# PER-TARGET DEFINITION - s62-execution-engine-lib
####################################################
set_target_properties(s62-execution-engine-lib PROPERTIES LINKER_LANGUAGE CXX)
set(SRC_DIR "${PROJECT_SOURCE_DIR}/src")
file (GLOB_RECURSE SOURCES "${SRC_DIR}/*.c*" 
)
target_include_directories( s62-execution-engine-lib PUBLIC "${PROJECT_SOURCE_DIR}/include" )
target_include_directories( s62-execution-engine-lib PUBLIC
	# s62-common
  "${PROJECT_SOURCE_DIR}/../s62-common/include"
	"${PROJECT_SOURCE_DIR}/../s62-common/include/common"
  "${PROJECT_SOURCE_DIR}/../s62-common/include/common/enums"
	"${PROJECT_SOURCE_DIR}/../s62-common/include/common/types"
  "${PROJECT_SOURCE_DIR}/../s62-common/include/common/operator"
	"${PROJECT_SOURCE_DIR}/../s62-common/include/common/value_operations" 
  # parser
	"${PROJECT_SOURCE_DIR}/../include/parser"
	"${PROJECT_SOURCE_DIR}/../include/parser/parsed_data"
  "${PROJECT_SOURCE_DIR}/include/parser/constraints"
	"${PROJECT_SOURCE_DIR}/include/parser/expression"
	"${PROJECT_SOURCE_DIR}/include/parser/query_node"
	"${PROJECT_SOURCE_DIR}/include/parser/statement"
	"${PROJECT_SOURCE_DIR}/include/parser/tableref"
  # s62-client
  "${PROJECT_SOURCE_DIR}/../s62-client/include/main"
)
target_sources( s62-execution-engine-lib PUBLIC "${SOURCES}" )
target_link_libraries( s62-execution-engine-lib
  s62-common-lib
  s62-main
  "-pthread -ldl -lboost_system -lboost_timer -lboost_date_time -lboost_filesystem"
)

####################################################
# TESTS
####################################################
file(GLOB ENGINE_TEST_SRC
  "${PROJECT_SOURCE_DIR}/test/bulkload_using_map.cpp"
)

foreach(test_file ${ENGINE_TEST_SRC})
  get_filename_component(prog_name ${test_file} NAME_WE)
  add_executable(${prog_name} ${test_file} )
  target_sources( ${prog_name} PUBLIC ${SOURCES} )
  target_compile_options(${prog_name} PRIVATE ${CXX_FLAGS})
	target_compile_definitions(${prog_name} PRIVATE ${CXX_DEFINITION})
  target_include_directories( ${prog_name} PUBLIC
    # s62-common
    "${PROJECT_SOURCE_DIR}/../s62-common/include/common" "${PROJECT_SOURCE_DIR}/../s62-common/include/common/enums"
    "${PROJECT_SOURCE_DIR}/../s62-common/include/common/types" "${PROJECT_SOURCE_DIR}/../s62-common/include/common/operator"
    "${PROJECT_SOURCE_DIR}/../s62-common/include/common/value_operations" "${PROJECT_SOURCE_DIR}/../s62-common/include/common/vector_operations"
    # test
    "${PROJECT_SOURCE_DIR}/test/"
  )
  target_link_libraries(${prog_name} PRIVATE
    s62-main
    s62-common-lib
    s62-graph-store-lib
    s62-execution-engine-lib
    nlohmann_json::nlohmann_json "-pthread -ldl -lboost_system -lboost_timer -lboost_date_time -lboost_filesystem"
  )
endforeach(test_file ${TILE_TEST_SRC})
