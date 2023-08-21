cmake_minimum_required(VERSION 3.16)

####################################################
# PROJECT METADATA 
####################################################
project(tbgpp-client)

####################################################
# ADD SUBDIRECTORIES
####################################################

####################################################
# COMPILATION FLAGS
####################################################
set(CMAKE_CXX_FLAGS "-std=c++17 -march=native -mavx2")
set(CMAKE_CXX_STANDARD 17)
if(NOT CMAKE_BUILD_TYPE)
	set(CMAKE_BUILD_TYPE Release)
endif()

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
)
target_include_directories(tbgpp-main PUBLIC
	# tbgpp-graph-store
	"${PROJECT_SOURCE_DIR}/../tbgpp-graph-store/include"
)
target_include_directories(tbgpp-main PUBLIC
	# tbgpp-execution-engine
	"${PROJECT_SOURCE_DIR}/../tbgpp-execution-engine/include"
	"${PROJECT_SOURCE_DIR}/../tbgpp-execution-engine/test"
)
target_include_directories(tbgpp-main PUBLIC
	# tbgpp-compiler
	"${PROJECT_SOURCE_DIR}/../tbgpp-compiler/include"
	"${PROJECT_SOURCE_DIR}/../tbgpp-compiler/include/planner"
	# kuzu
	"${PROJECT_SOURCE_DIR}/../tbgpp-compiler/kuzu/include"
	# gpdb
	"${PROJECT_SOURCE_DIR}/../tbgpp-compiler/gpdb/src/include"
	# gporca
	"${PROJECT_SOURCE_DIR}/../tbgpp-compiler/gpdb/src/backend/gporca/libgpos/include"
	"${PROJECT_SOURCE_DIR}/../tbgpp-compiler/gpdb/src/backend/gporca/libnaucrates/include"
	"${PROJECT_SOURCE_DIR}/../tbgpp-compiler/gpdb/src/backend/gporca/libgpdbcost/include"
	"${PROJECT_SOURCE_DIR}/../tbgpp-compiler/gpdb/src/backend/gporca/libgpopt/include"
	"${PROJECT_SOURCE_DIR}/../tbgpp-compiler/gpdb/src/backend/gporca/server/include"
	# third_party
	"${PROJECT_SOURCE_DIR}/../tbgpp-compiler/third_party/antlr4_cypher/antlr4"
	"${PROJECT_SOURCE_DIR}/../tbgpp-compiler/third_party/antlr4_runtime/src"
)
target_include_directories(tbgpp-main PUBLIC
	# tbgpp-common
	"${PROJECT_SOURCE_DIR}/../tbgpp-common/include"
	# common
	"${PROJECT_SOURCE_DIR}/../tbgpp-common/include/common" "${PROJECT_SOURCE_DIR}/../tbgpp-common/include/common/enums"
	"${PROJECT_SOURCE_DIR}/../tbgpp-common/include/common/types" "${PROJECT_SOURCE_DIR}/../tbgpp-common/include/common/operator"
	"${PROJECT_SOURCE_DIR}/../tbgpp-common/include/common/value_operations"
	#"${PROJECT_SOURCE_DIR}/../tbgpp-common/include/common/vector_operations"
	# parser
	"${PROJECT_SOURCE_DIR}/include/parser"
	"${PROJECT_SOURCE_DIR}/include/parser/parsed_data" 
)
file (GLOB_RECURSE SYSTEM_SOURCES "${PROJECT_SOURCE_DIR}/src/*.c*")
target_sources(tbgpp-main PUBLIC ${SYSTEM_SOURCES})
target_link_libraries(tbgpp-main "-lrt -lpthread -ltbb -fopenmp -laio -lnuma -lhwloc")
target_link_libraries(tbgpp-main tbgpp-common-lib)
target_link_libraries(tbgpp-main tbgpp-graph-store-lib)
target_link_libraries(tbgpp-main tbgpp-execution-engine-lib)
target_link_libraries(tbgpp-main tbgpp-compiler)

target_compile_options(tbgpp-main PUBLIC ${CXX_FLAGS})
target_compile_definitions(tbgpp-main PUBLIC ${CXX_DEFINITION})

####################################################
# TESTS
####################################################
file(GLOB CLIENT_TEST_SRC
  "${PROJECT_SOURCE_DIR}/test/TurboGraph-S62.cpp"
  #"${PROJECT_SOURCE_DIR}/test/dump_s62_mda.cpp"
)

foreach(test_file ${CLIENT_TEST_SRC})
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
endforeach(test_file ${CLIENT_TEST_SRC})
