cmake_minimum_required(VERSION 3.16)

####################################################
# PROJECT METADATA 
####################################################
project(compiler)

####################################################
# ADD SUBDIRECTORIES
####################################################
#SET(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DGPOS_DEBUG")

add_subdirectory(kuzu)
add_subdirectory(orca)

####################################################
# FIND PACKAGES
####################################################

####################################################
# DECLARE TARGETS
####################################################
add_library(compiler STATIC)

####################################################
# GLOBAL DEFINITION
####################################################

####################################################
# PER-TARGET DEFINITION - compiler
####################################################
target_include_directories(compiler PUBLIC
	# kuzu
	"${PROJECT_SOURCE_DIR}/kuzu/include"
	# client
	"${PROJECT_SOURCE_DIR}/../client/include/main"
	# common
	"${PROJECT_SOURCE_DIR}/../common/include"
	"${PROJECT_SOURCE_DIR}/../common/include/common"
  	"${PROJECT_SOURCE_DIR}/../common/include/common/enums"
	"${PROJECT_SOURCE_DIR}/../common/include/common/types"
  	"${PROJECT_SOURCE_DIR}/../common/include/common/operator"
	"${PROJECT_SOURCE_DIR}/../common/include/common/value_operations" 
	"${PROJECT_SOURCE_DIR}/../common/third_party/TreePrinter/TreePrinter/src"
	# execution
	"${PROJECT_SOURCE_DIR}/../execution/include"
	"${PROJECT_SOURCE_DIR}/../execution/include/execution"

	# storage
	"${PROJECT_SOURCE_DIR}/../storage/include"
)

file(GLOB_RECURSE SOURCES_MD "${PROJECT_SOURCE_DIR}/mdprovider/*.c*")
file(GLOB_RECURSE SOURCES_PLANNER "${PROJECT_SOURCE_DIR}/planner/*.c*")
list(APPEND SOURCES ${SOURCES_MD} ${SOURCES_PLANNER})
target_sources(compiler PUBLIC ${SOURCES})
target_link_libraries(compiler common-lib tbgpp-main execution-lib)
target_link_libraries(compiler "-lrt -lpthread -ltbb -fopenmp -laio -lnuma -lhwloc")
# orca
target_link_libraries(compiler orca)
target_link_libraries(compiler orca naucrates gpdbcost gpopt)
# kuzu
target_link_libraries(compiler kuzu)
# third_party
target_link_libraries(compiler antlr4_cypher antlr4_runtime)
target_compile_options(compiler PUBLIC ${CXX_FLAGS})
target_compile_definitions(compiler PUBLIC ${CXX_DEFINITION})