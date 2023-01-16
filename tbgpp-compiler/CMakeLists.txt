cmake_minimum_required(VERSION 3.16)

####################################################
# PROJECT METADATA 
####################################################
project(tbgpp-compiler)

####################################################
# ADD SUBDIRECTORIES
####################################################
add_subdirectory(third_party)

####################################################
# COMPILATION FLAGS
####################################################
set(CMAKE_CXX_FLAGS "-std=c++17")
set(CMAKE_CXX_STANDARD 17)
if(NOT CMAKE_BUILD_TYPE)
	set(CMAKE_BUILD_TYPE Release)
endif()

####################################################
# DECLARE TARGETS
####################################################
add_library(tbgpp-compiler STATIC)

####################################################
# GLOBAL DEFINITION
####################################################

####################################################
# PER-TARGET DEFINITION - tbgpp-compiler
####################################################
target_include_directories(tbgpp-compiler PUBLIC
	# include
	"${PROJECT_SOURCE_DIR}/include"
	# third_party
	"${PROJECT_SOURCE_DIR}/third_party/kuzu/include"
	"${PROJECT_SOURCE_DIR}/third_party/antlr4_cypher/include"
	"${PROJECT_SOURCE_DIR}/third_party/antlr4_runtime/src"
	# tbgpp-common
	"${PROJECT_SOURCE_DIR}/../tbgpp-common/include"
	"${PROJECT_SOURCE_DIR}/../tbgpp-common/include/common"
  	"${PROJECT_SOURCE_DIR}/../tbgpp-common/include/common/enums"
	"${PROJECT_SOURCE_DIR}/../tbgpp-common/include/common/types"
  	"${PROJECT_SOURCE_DIR}/../tbgpp-common/include/common/operator"
	"${PROJECT_SOURCE_DIR}/../tbgpp-common/include/common/value_operations" 
	"${PROJECT_SOURCE_DIR}/../tbgpp-common/third_party/TreePrinter/TreePrinter/src"
)

file (GLOB_RECURSE SOURCES "${PROJECT_SOURCE_DIR}/src/*.c*")
target_sources(tbgpp-compiler PUBLIC ${SOURCES})
#target_link_libraries(tbgpp-compiler tbgpp-common-lib)
target_link_libraries(tbgpp-compiler pthread aio numa rt hwloc tbb)
target_link_libraries(tbgpp-compiler antlr4_cypher antlr4_runtime kuzu)
target_compile_options(tbgpp-compiler PUBLIC ${CXX_FLAGS})
target_compile_definitions(tbgpp-compiler PUBLIC ${CXX_DEFINITION})

####################################################
# TESTS
####################################################
file(GLOB COMPILER_TEST_SRC
  "${PROJECT_SOURCE_DIR}/test/kuzu_integration_test.cpp"
)

foreach(test_file ${COMPILER_TEST_SRC})
	get_filename_component(prog_name ${test_file} NAME_WE)
	add_executable(${prog_name} ${test_file} )
	target_sources( ${prog_name} PUBLIC ${SOURCES} )
	target_compile_options(${prog_name} PRIVATE ${CXX_FLAGS})
	target_compile_definitions(${prog_name} PRIVATE ${CXX_DEFINITION})
	target_include_directories( ${prog_name} PUBLIC
		# test
		"${PROJECT_SOURCE_DIR}/test/"
	)
	target_link_libraries(${prog_name} PRIVATE
		tbgpp-compiler
		"-pthread -ldl"
	)
endforeach(test_file ${TILE_TEST_SRC})