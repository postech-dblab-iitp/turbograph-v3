cmake_minimum_required(VERSION 3.16)

####################################################
# PROJECT METADATA 
####################################################
project(compiler)

####################################################
# ADD SUBDIRECTORIES
####################################################
#SET(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DGPOS_DEBUG")

add_subdirectory(third_party)
add_subdirectory(kuzu)
add_subdirectory(gpdb)
add_subdirectory(gpdb/src/backend/gporca)

####################################################
# COMPILATION FLAGS
####################################################
set(CMAKE_CXX_STANDARD 17)
if(NOT CMAKE_BUILD_TYPE)
	set(CMAKE_BUILD_TYPE Release)
endif()
#SET(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DGPOS_DEBUG")

# apply gporca configurations
add_compile_options(-DUSE_CMAKE)

####################################################
# FIND PACKAGES
####################################################
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake")

# dependencies - boost
#set(Boost_DEBUG 1)

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
add_library(compiler STATIC)

####################################################
# GLOBAL DEFINITION
####################################################

####################################################
# PER-TARGET DEFINITION - compiler
####################################################
target_include_directories(compiler PUBLIC
	# include
	"${PROJECT_SOURCE_DIR}/include"
	"${PROJECT_SOURCE_DIR}/include/mdprovider"
	"${PROJECT_SOURCE_DIR}/include/planner"
	# kuzu
	"${PROJECT_SOURCE_DIR}/kuzu/include"
	# gpdb
	"${PROJECT_SOURCE_DIR}/gpdb/src/include"
	"${PROJECT_SOURCE_DIR}/gpdb/src/include/gpopt"
	# gporca
	"${PROJECT_SOURCE_DIR}/gpdb/src/backend/gporca/libgpos/include"
	"${PROJECT_SOURCE_DIR}/gpdb/src/backend/gporca/libnaucrates/include"
	"${PROJECT_SOURCE_DIR}/gpdb/src/backend/gporca/libgpdbcost/include"
	"${PROJECT_SOURCE_DIR}/gpdb/src/backend/gporca/libgpopt/include"
	"${PROJECT_SOURCE_DIR}/gpdb/src/backend/gporca/server/include"
	# third_party
	"${PROJECT_SOURCE_DIR}/third_party/antlr4_cypher/antlr4"
	"${PROJECT_SOURCE_DIR}/third_party/antlr4_runtime/src"
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

file (GLOB_RECURSE SOURCES "${PROJECT_SOURCE_DIR}/src/*.c*")
target_sources(compiler PUBLIC ${SOURCES})
target_link_libraries(compiler common-lib tbgpp-main execution-lib)
target_link_libraries(compiler "-lrt -lpthread -ltbb -fopenmp -laio -lnuma -lhwloc")
# gpdb
target_link_libraries(compiler gpdb)
target_link_libraries(compiler gpos naucrates gpdbcost gpopt)
# kuzu
target_link_libraries(compiler kuzu)
# third_party
target_link_libraries(compiler antlr4_cypher antlr4_runtime)
target_compile_options(compiler PUBLIC ${CXX_FLAGS})
target_compile_definitions(compiler PUBLIC ${CXX_DEFINITION})