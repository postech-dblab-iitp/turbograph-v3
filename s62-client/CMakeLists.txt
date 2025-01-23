cmake_minimum_required(VERSION 3.16)

####################################################
# PROJECT METADATA 
####################################################
project(s62-client)

####################################################
# ADD SUBDIRECTORIES
####################################################

####################################################
# COMPILATION FLAGS
####################################################
set(CMAKE_CXX_STANDARD 17)
if(NOT CMAKE_BUILD_TYPE)
	set(CMAKE_BUILD_TYPE Release)
endif()

####################################################
# DECLARE TARGETS
####################################################
add_library(s62-main STATIC)

####################################################
# GLOBAL DEFINITION
####################################################

####################################################
# PER-TARGET DEFINITION - s62-main
####################################################
target_include_directories(s62-main PUBLIC
	"${PROJECT_SOURCE_DIR}/include"
	"${PROJECT_SOURCE_DIR}/include/main"	
	"${PROJECT_SOURCE_DIR}/include/connection"	
)
target_include_directories(s62-main PUBLIC
	# s62-graph-store
	"${PROJECT_SOURCE_DIR}/../s62-graph-store/include"
)
target_include_directories(s62-main PUBLIC
	# s62-execution-engine
	"${PROJECT_SOURCE_DIR}/../s62-execution-engine/include"
	"${PROJECT_SOURCE_DIR}/../s62-execution-engine/test"
)
target_include_directories(s62-main PUBLIC
	# s62-compiler
	"${PROJECT_SOURCE_DIR}/../s62-compiler/include"
	"${PROJECT_SOURCE_DIR}/../s62-compiler/include/planner"
	# kuzu
	"${PROJECT_SOURCE_DIR}/../s62-compiler/kuzu/include"
	# gpdb
	"${PROJECT_SOURCE_DIR}/../s62-compiler/gpdb/src/include"
	# gporca
	"${PROJECT_SOURCE_DIR}/../s62-compiler/gpdb/src/backend/gporca/libgpos/include"
	"${PROJECT_SOURCE_DIR}/../s62-compiler/gpdb/src/backend/gporca/libnaucrates/include"
	"${PROJECT_SOURCE_DIR}/../s62-compiler/gpdb/src/backend/gporca/libgpdbcost/include"
	"${PROJECT_SOURCE_DIR}/../s62-compiler/gpdb/src/backend/gporca/libgpopt/include"
	"${PROJECT_SOURCE_DIR}/../s62-compiler/gpdb/src/backend/gporca/server/include"
	# third_party
	"${PROJECT_SOURCE_DIR}/../s62-compiler/third_party/antlr4_cypher/antlr4"
	"${PROJECT_SOURCE_DIR}/../s62-compiler/third_party/antlr4_runtime/src"
)
target_include_directories(s62-main PUBLIC
	# s62-common
	"${PROJECT_SOURCE_DIR}/../s62-common/include"
	# common
	"${PROJECT_SOURCE_DIR}/../s62-common/include/common" "${PROJECT_SOURCE_DIR}/../s62-common/include/common/enums"
	"${PROJECT_SOURCE_DIR}/../s62-common/include/common/types" "${PROJECT_SOURCE_DIR}/../s62-common/include/common/operator"
	"${PROJECT_SOURCE_DIR}/../s62-common/include/common/value_operations"
	#"${PROJECT_SOURCE_DIR}/../s62-common/include/common/vector_operations"
	# parser
	"${PROJECT_SOURCE_DIR}/include/parser"
	"${PROJECT_SOURCE_DIR}/include/parser/parsed_data" 
)
file (GLOB_RECURSE SYSTEM_SOURCES "${PROJECT_SOURCE_DIR}/src/*.c*")
target_sources(s62-main PUBLIC ${SYSTEM_SOURCES})
target_link_libraries(s62-main "-lrt -lpthread -ltbb -fopenmp -laio -lnuma -lhwloc -ltcmalloc")
target_link_libraries(s62-main s62-common-lib)
target_link_libraries(s62-main s62-graph-store-lib)
target_link_libraries(s62-main s62-execution-engine-lib)
target_link_libraries(s62-main s62-compiler)

target_compile_options(s62-main PUBLIC ${CXX_FLAGS})
target_compile_definitions(s62-main PUBLIC ${CXX_DEFINITION})

####################################################
# TESTS
####################################################
file(GLOB CLIENT_TEST_SRC
  "${PROJECT_SOURCE_DIR}/test/s62_cli.cpp"
)

foreach(test_file ${CLIENT_TEST_SRC})
  get_filename_component(prog_name ${test_file} NAME_WE)
  add_executable(${prog_name} ${test_file} )
  target_sources( ${prog_name} PUBLIC ${SOURCES} )
  target_sources( ${prog_name} PUBLIC ${TEST_SOURCES}  ) 
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
    nlohmann_json::nlohmann_json "-pthread -ldl -lboost_system -lboost_timer -lboost_date_time -lboost_filesystem -ltbb"
  )
endforeach(test_file ${CLIENT_TEST_SRC})
