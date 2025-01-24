cmake_minimum_required(VERSION 3.16)

####################################################
# PROJECT METADATA 
####################################################
project(storage)

####################################################
# COMPILATION FLAGS
####################################################ㄴ
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -g")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3")

if(NOT CMAKE_BUILD_TYPE)
	set(CMAKE_BUILD_TYPE Release)
endif()

####################################################
# FIND PACKAGES
####################################################

find_package(TBB REQUIRED)
find_package(OpenMP REQUIRED)
set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS}")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${OpenMP_EXE_LINKER_FLAGS}")
find_package(Catch2 REQUIRED)

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
# TODO need to get rid of global definition!!!!
include_directories(
  # include  
  "${PROJECT_SOURCE_DIR}/include"
  # cache
  "${PROJECT_SOURCE_DIR}/include/cache" 
  "${PROJECT_SOURCE_DIR}/include/cache/disk_aio"
  # catalog
  "${PROJECT_SOURCE_DIR}/include/catalog"
  "${PROJECT_SOURCE_DIR}/include/catalog/default"
  "${PROJECT_SOURCE_DIR}/include/catalog/catalog_entry"
  # schemaless
  "${PROJECT_SOURCE_DIR}/include/schemaless"
  # statistics
  "${PROJECT_SOURCE_DIR}/include/statistics"
)

####################################################
# PER-TARGET DEFINITION - storage-lib
####################################################
set(SRC_DIR "${PROJECT_SOURCE_DIR}/src")
file (GLOB_RECURSE SYSTEM_SOURCES "${SRC_DIR}/*.c*")
SET(TBGPP_SYSTEM_SOURCES ${SYSTEM_SOURCES})
target_sources(storage-lib PUBLIC ${TBGPP_SYSTEM_SOURCES})
target_include_directories( storage-lib PUBLIC 
  # include  
  "${PROJECT_SOURCE_DIR}/include"
  # cache
  "${PROJECT_SOURCE_DIR}/include/cache" 
  "${PROJECT_SOURCE_DIR}/include/cache/disk_aio"
  # catalog
  "${PROJECT_SOURCE_DIR}/include/catalog"
  "${PROJECT_SOURCE_DIR}/include/catalog/default"
  "${PROJECT_SOURCE_DIR}/include/catalog/catalog_entry"
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
set(CACHE_SRC_DIR "${PROJECT_SOURCE_DIR}/src/cache")
file (GLOB_RECURSE CACHE_SYSTEM_SOURCES "${CACHE_SRC_DIR}/*.c*")
SET(CACHE_LIB_SRC ${CACHE_SYSTEM_SOURCES})
target_sources(lightning PUBLIC ${CACHE_LIB_SRC})

target_link_libraries(lightning "-lrt -lpthread -ltbb -fopenmp -laio -lnuma -lhwloc")
target_link_libraries(lightning common-lib)

####################################################
# PER-TARGET DEFINITION - store
####################################################
target_sources(store PUBLIC "${PROJECT_SOURCE_DIR}/test/test_store.cc")
target_include_directories( store PUBLIC
  # include  
  "${PROJECT_SOURCE_DIR}/include"
  # cache
  "${PROJECT_SOURCE_DIR}/include/cache" 
  "${PROJECT_SOURCE_DIR}/include/cache/disk_aio"
  # catalog
  "${PROJECT_SOURCE_DIR}/include/catalog"
  "${PROJECT_SOURCE_DIR}/include/catalog/default"
  "${PROJECT_SOURCE_DIR}/include/catalog/catalog_entry"
)
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
# TODO need to link headers and other dependent libraries!!!

file(GLOB CATALOG_TEST_SRC
  "${PROJECT_SOURCE_DIR}/test/catalog_test_catalog_server.cpp"
)

if (TRUE)
foreach(test_file ${CATALOG_TEST_SRC})
  get_filename_component(prog_name ${test_file} NAME_WE)
  add_executable(${prog_name} ${test_file})
  target_compile_options(${prog_name} PRIVATE ${CXX_FLAGS})
	target_compile_definitions(${prog_name} PRIVATE ${CXX_DEFINITION})
  target_link_libraries(${prog_name} PRIVATE storage-lib  Catch2::Catch2WithMain)
endforeach(test_file ${CATALOG_TEST_SRC})
endif()