cmake_minimum_required(VERSION 3.16)

####################################################
# PROJECT METADATA 
####################################################
project(client)

####################################################
# ADD SUBDIRECTORIES
####################################################

####################################################
# DECLARE TARGETS
####################################################
add_library(tbgpp-main STATIC)

####################################################
# GLOBAL DEFINITION
####################################################

####################################################
# PER-TARGET DEFINITION - tbgpp-main
####################################################
target_include_directories(tbgpp-main PUBLIC
	"${PROJECT_SOURCE_DIR}/include"
	"${PROJECT_SOURCE_DIR}/include/main"	
	"${PROJECT_SOURCE_DIR}/include/connection"	
)
target_include_directories(tbgpp-main PUBLIC
	# storage
	"${PROJECT_SOURCE_DIR}/../storage/include"
)
target_include_directories(tbgpp-main PUBLIC
	# execution
	"${PROJECT_SOURCE_DIR}/../execution/include"
	"${PROJECT_SOURCE_DIR}/../execution/test"
)
target_include_directories(tbgpp-main PUBLIC
	# compiler
	"${PROJECT_SOURCE_DIR}/../compiler/include"
	"${PROJECT_SOURCE_DIR}/../compiler/include/planner"
	# kuzu
	"${PROJECT_SOURCE_DIR}/../compiler/kuzu/include"
	# gpdb
	"${PROJECT_SOURCE_DIR}/../compiler/gpdb/src/include"
	# gporca
	"${PROJECT_SOURCE_DIR}/../compiler/gpdb/src/backend/gporca/libgpos/include"
	"${PROJECT_SOURCE_DIR}/../compiler/gpdb/src/backend/gporca/libnaucrates/include"
	"${PROJECT_SOURCE_DIR}/../compiler/gpdb/src/backend/gporca/libgpdbcost/include"
	"${PROJECT_SOURCE_DIR}/../compiler/gpdb/src/backend/gporca/libgpopt/include"
	"${PROJECT_SOURCE_DIR}/../compiler/gpdb/src/backend/gporca/server/include"
	# third_party
	"${PROJECT_SOURCE_DIR}/../compiler/third_party/antlr4_cypher/antlr4"
	"${PROJECT_SOURCE_DIR}/../compiler/third_party/antlr4_runtime/src"
)
target_include_directories(tbgpp-main PUBLIC
	# common
	"${PROJECT_SOURCE_DIR}/../common/include"
	# common
	"${PROJECT_SOURCE_DIR}/../common/include/common" "${PROJECT_SOURCE_DIR}/../common/include/common/enums"
	"${PROJECT_SOURCE_DIR}/../common/include/common/types" "${PROJECT_SOURCE_DIR}/../common/include/common/operator"
	"${PROJECT_SOURCE_DIR}/../common/include/common/value_operations"
	#"${PROJECT_SOURCE_DIR}/../common/include/common/vector_operations"
	# parser
	"${PROJECT_SOURCE_DIR}/include/parser"
	"${PROJECT_SOURCE_DIR}/include/parser/parsed_data" 
)
file (GLOB_RECURSE SYSTEM_SOURCES "${PROJECT_SOURCE_DIR}/src/*.c*")
target_sources(tbgpp-main PUBLIC ${SYSTEM_SOURCES})
target_link_libraries(tbgpp-main "-lrt -lpthread -ltbb -fopenmp -laio -lnuma -lhwloc -ltcmalloc")
target_link_libraries(tbgpp-main common-lib)
target_link_libraries(tbgpp-main storage-lib)
target_link_libraries(tbgpp-main execution-lib)
target_link_libraries(tbgpp-main compiler)

target_compile_options(tbgpp-main PUBLIC ${CXX_FLAGS})
target_compile_definitions(tbgpp-main PUBLIC ${CXX_DEFINITION})

####################################################
# TESTS
####################################################
file(GLOB CLIENT_TEST_SRC
  "${PROJECT_SOURCE_DIR}/test/TurboGraph-S62.cpp"
#   "${PROJECT_SOURCE_DIR}/test/connection_apis_test.cpp"
#   "${PROJECT_SOURCE_DIR}/test/connection_client_test.cpp"
#   "${PROJECT_SOURCE_DIR}/test/connection_server_run.cpp"
#   "${PROJECT_SOURCE_DIR}/test/dump_s62_mda.cpp"
)

foreach(test_file ${CLIENT_TEST_SRC})
  get_filename_component(prog_name ${test_file} NAME_WE)
  add_executable(${prog_name} ${test_file} )
  target_sources( ${prog_name} PUBLIC ${SOURCES} )
  target_sources( ${prog_name} PUBLIC ${TEST_SOURCES}  ) 
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
    nlohmann_json::nlohmann_json "-pthread -ldl -lboost_system -lboost_timer -lboost_date_time -lboost_filesystem -ltbb"
  )
endforeach(test_file ${CLIENT_TEST_SRC})
