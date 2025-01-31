cmake_minimum_required(VERSION 3.16)

###################################################
# PROJECT METADATA
###################################################
project(execution)

####################################################
# FIND PACKAGES
####################################################

####################################################
# DECLARE TARGETS
####################################################
add_library(execution-lib STATIC)

####################################################
# GLOBAL DEFINITION
####################################################
add_definitions(-DDUCKDB_SOURCE_ID="\""1"\"")
add_definitions(-DDUCKDB_VERSION="\""1"\"")

####################################################
# PER-TARGET DEFINITION - execution-lib
####################################################
set_target_properties(execution-lib PROPERTIES LINKER_LANGUAGE CXX)
set(SRC_DIR "${PROJECT_SOURCE_DIR}/src")
file (GLOB_RECURSE SOURCES "${SRC_DIR}/*.c*" 
	# for debugging purpose, temporary
	#"${PROJECT_SOURCE_DIR}/test/plans/ldbc_snb_interactive/*.c*"
	#"${PROJECT_SOURCE_DIR}/test/plans/*.c*"
)
target_include_directories( execution-lib PUBLIC
	# common
  "${PROJECT_SOURCE_DIR}/../common/include"
	"${PROJECT_SOURCE_DIR}/../common/include/common"
  "${PROJECT_SOURCE_DIR}/../common/include/common/enums"
	"${PROJECT_SOURCE_DIR}/../common/include/common/types"
  "${PROJECT_SOURCE_DIR}/../common/include/common/operator"
	"${PROJECT_SOURCE_DIR}/../common/include/common/value_operations" 
  # parser
	"${PROJECT_SOURCE_DIR}/../include/parser"
	"${PROJECT_SOURCE_DIR}/../include/parser/parsed_data"
  "${PROJECT_SOURCE_DIR}/include/parser/constraints"
	"${PROJECT_SOURCE_DIR}/include/parser/expression"
	"${PROJECT_SOURCE_DIR}/include/parser/query_node"
	"${PROJECT_SOURCE_DIR}/include/parser/statement"
	"${PROJECT_SOURCE_DIR}/include/parser/tableref"
  # client
  "${PROJECT_SOURCE_DIR}/../client/include/main"
)
target_sources( execution-lib PUBLIC "${SOURCES}" )
target_link_libraries( execution-lib
  common-lib
  tbgpp-main
  "-pthread -ldl -lboost_system -lboost_timer -lboost_date_time -lboost_filesystem"
)

####################################################
# TESTS
####################################################
file(GLOB ENGINE_TEST_SRC
  "${PROJECT_SOURCE_DIR}/test/bulkload_using_map.cpp"
)
file (GLOB_RECURSE SOURCES "${SRC_DIR}/*.c*")

foreach(test_file ${ENGINE_TEST_SRC})
  get_filename_component(prog_name ${test_file} NAME_WE)
  add_executable(${prog_name} ${test_file} )
  target_sources( ${prog_name} PUBLIC ${SOURCES} )
  target_compile_options(${prog_name} PRIVATE ${CXX_FLAGS})
	target_compile_definitions(${prog_name} PRIVATE ${CXX_DEFINITION})
  target_include_directories( ${prog_name} PUBLIC
    # common
    "${PROJECT_SOURCE_DIR}/../common/include/common" "${PROJECT_SOURCE_DIR}/../common/include/common/enums"
    "${PROJECT_SOURCE_DIR}/../common/include/common/types" "${PROJECT_SOURCE_DIR}/../common/include/common/operator"
    "${PROJECT_SOURCE_DIR}/../common/include/common/value_operations" "${PROJECT_SOURCE_DIR}/../common/include/common/vector_operations"
    # test
    "${PROJECT_SOURCE_DIR}/test/"
  )
  target_link_libraries(${prog_name} PRIVATE
    tbgpp-main
    common-lib
    storage-lib
    execution-lib
    nlohmann_json::nlohmann_json "-pthread -ldl -lboost_system -lboost_timer -lboost_date_time -lboost_filesystem"
  )
endforeach(test_file ${TILE_TEST_SRC})
