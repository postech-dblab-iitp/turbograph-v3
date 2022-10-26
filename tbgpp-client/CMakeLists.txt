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
set(CMAKE_CXX_FLAGS "-std=c++17")
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
target_link_libraries(tbgpp-main pthread aio numa rt hwloc tbb)
target_link_libraries(tbgpp-main tbgpp-common-lib)
target_link_libraries(tbgpp-main tbgpp-graph-store-lib)
target_link_libraries(tbgpp-main tbgpp-execution-engine-lib)

target_compile_options(tbgpp-main PUBLIC ${CXX_FLAGS})
target_compile_definitions(tbgpp-main PUBLIC ${CXX_DEFINITION})

####################################################
# TESTS
####################################################
