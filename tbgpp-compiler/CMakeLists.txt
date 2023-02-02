cmake_minimum_required(VERSION 3.16)

####################################################
# PROJECT METADATA 
####################################################
project(tbgpp-compiler)

####################################################
# ADD SUBDIRECTORIES
####################################################
add_subdirectory(third_party)
add_subdirectory(kuzu)
add_subdirectory(gpdb)
add_subdirectory(gpdb/src/backend/gporca)

####################################################
# COMPILATION FLAGS
####################################################
set(CMAKE_CXX_FLAGS "-std=c++17")
set(CMAKE_CXX_STANDARD 17)
if(NOT CMAKE_BUILD_TYPE)
	set(CMAKE_BUILD_TYPE Release)
endif()

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
	# tbgpp-client
	"${PROJECT_SOURCE_DIR}/../tbgpp-client/include/main"
	# tbgpp-common
	"${PROJECT_SOURCE_DIR}/../tbgpp-common/include"
	"${PROJECT_SOURCE_DIR}/../tbgpp-common/include/common"
  	"${PROJECT_SOURCE_DIR}/../tbgpp-common/include/common/enums"
	"${PROJECT_SOURCE_DIR}/../tbgpp-common/include/common/types"
  	"${PROJECT_SOURCE_DIR}/../tbgpp-common/include/common/operator"
	"${PROJECT_SOURCE_DIR}/../tbgpp-common/include/common/value_operations" 
	"${PROJECT_SOURCE_DIR}/../tbgpp-common/third_party/TreePrinter/TreePrinter/src"
	# tbgpp-graph-store
	"${PROJECT_SOURCE_DIR}/../tbgpp-graph-store/include"
)

file (GLOB_RECURSE SOURCES "${PROJECT_SOURCE_DIR}/src/*.c*")
target_sources(tbgpp-compiler PUBLIC ${SOURCES})
target_link_libraries(tbgpp-compiler tbgpp-common-lib tbgpp-main)
target_link_libraries(tbgpp-compiler "-lrt -lpthread -ltbb -fopenmp -laio -lnuma -lhwloc")
# gpdb
target_link_libraries(tbgpp-compiler gpdb)
target_link_libraries(tbgpp-compiler gpos naucrates gpdbcost gpopt gporca_test_lib)
# kuzu
target_link_libraries(tbgpp-compiler kuzu)
# third_party
target_link_libraries(tbgpp-compiler antlr4_cypher antlr4_runtime)
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
