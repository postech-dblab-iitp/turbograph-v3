cmake_minimum_required(VERSION 3.16)

####################################################
# PROJECT METADATA 
####################################################
project(storage)

####################################################
# DECLARE TARGETS
####################################################
add_library(lightning STATIC)
add_library(storage-lib STATIC)
add_executable(store)

####################################################
# GLOBAL DEFINITION
####################################################
link_libraries(pthread aio numa rt hwloc tbb)

####################################################
# PER-TARGET DEFINITION - storage-lib
####################################################
file (GLOB_RECURSE SYSTEM_SOURCES "${PROJECT_SOURCE_DIR}/*.c*")
SET(TBGPP_SYSTEM_SOURCES ${SYSTEM_SOURCES})
target_sources(storage-lib PUBLIC ${TBGPP_SYSTEM_SOURCES})
target_include_directories( storage-lib PUBLIC 
  # client
  "${PROJECT_SOURCE_DIR}/../client/include/main"
  # execution-engine
  "${PROJECT_SOURCE_DIR}/../execution/include/execution"
  "${PROJECT_SOURCE_DIR}/../execution/include/storage"
)
target_link_libraries(storage-lib common-lib)
target_link_libraries(storage-lib tbgpp-main)


####################################################
# PER-TARGET DEFINITION - lightning
####################################################
set(CACHE_SRC_DIR "${PROJECT_SOURCE_DIR}/cache")
file (GLOB_RECURSE CACHE_SYSTEM_SOURCES "${CACHE_SRC_DIR}/*.c*")
SET(CACHE_LIB_SRC ${CACHE_SYSTEM_SOURCES})
target_sources(lightning PUBLIC ${CACHE_LIB_SRC})

target_link_libraries(lightning "-lrt -lpthread -ltbb -fopenmp -laio -lnuma -lhwloc")
target_link_libraries(lightning common-lib)

####################################################
# PER-TARGET DEFINITION - store
####################################################
target_sources(store PUBLIC "${PROJECT_SOURCE_DIR}/test/test_store.cc")
target_include_directories(store PUBLIC
	# common
  "${PROJECT_SOURCE_DIR}/../common/include"
  # common
	"${PROJECT_SOURCE_DIR}/../common/include/common" "${PROJECT_SOURCE_DIR}/../common/include/common/enums"
	"${PROJECT_SOURCE_DIR}/../common/include/common/types" "${PROJECT_SOURCE_DIR}/../common/include/common/operator"
	"${PROJECT_SOURCE_DIR}/../common/include/common/value_operations"
  #"${PROJECT_SOURCE_DIR}/../common/include/common/vector_operations"
  # parser
	"${PROJECT_SOURCE_DIR}/../include/parser"
	"${PROJECT_SOURCE_DIR}/../include/parser/parsed_data"
)
target_link_libraries(store lightning)
target_link_libraries(store common-lib)
target_link_libraries(store tbgpp-main)


####################################################
# TESTS
####################################################

file(GLOB CATALOG_TEST_SRC
  "${PROJECT_SOURCE_DIR}/test/catalog_test_catalog_server.cpp"
)

if (TRUE)
foreach(test_file ${CATALOG_TEST_SRC})
  get_filename_component(prog_name ${test_file} NAME_WE)
  add_executable(${prog_name} ${test_file})
  target_compile_options(${prog_name} PRIVATE ${CXX_FLAGS})
	target_compile_definitions(${prog_name} PRIVATE ${CXX_DEFINITION})
  target_link_libraries(${prog_name} PRIVATE storage-lib)
endforeach(test_file ${CATALOG_TEST_SRC})
endif()