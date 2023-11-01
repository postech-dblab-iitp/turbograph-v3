cmake_minimum_required(VERSION 3.16)

####################################################
# PROJECT METADATA 
####################################################
project(tbgpp-graph-store)

####################################################
# COMPILATION FLAGS
####################################################
set(CMAKE_CXX_FLAGS "-std=c++17 -march=native -mavx2 -w")
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -g")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3")

if(NOT CMAKE_BUILD_TYPE)
	set(CMAKE_BUILD_TYPE Release)
endif()

####################################################
# FIND PACKAGES
####################################################

find_package(Boost REQUIRED)
include_directories(${Boost_INCLUDE_DIRS})
link_directories(${Boost_LIBRARY_DIRS})
link_libraries(${Boost_LIBRARIES})

find_package(OpenMP REQUIRED)
set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS}")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${OpenMP_EXE_LINKER_FLAGS}")
find_package(Catch2 REQUIRED)
find_package(MPI REQUIRED)

find_package(Threads REQUIRED)
link_libraries(Threads::Threads)
include_directories(${TBB_INCLUDE_DIRS})
link_directories(${TBB_LIBRARY_DIRS})
link_libraries(${TBB_LIBRARIES})

####################################################
# DECLARE TARGETS
####################################################
add_library(lightning STATIC)
add_library(tbgpp-graph-store-lib STATIC)
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
  # catalog
  "${PROJECT_SOURCE_DIR}/include/schemaless"
)

####################################################
# PER-TARGET DEFINITION - tbgpp-graph-store-lib
####################################################
set(SRC_DIR "${PROJECT_SOURCE_DIR}/src")
file (GLOB_RECURSE SYSTEM_SOURCES "${SRC_DIR}/*.c*")
SET(TBGPP_SYSTEM_SOURCES ${SYSTEM_SOURCES})
target_sources(tbgpp-graph-store-lib PUBLIC ${TBGPP_SYSTEM_SOURCES})
target_include_directories( tbgpp-graph-store-lib PUBLIC 
  # include  
  "${PROJECT_SOURCE_DIR}/include"
  # cache
  "${PROJECT_SOURCE_DIR}/include/cache" 
  "${PROJECT_SOURCE_DIR}/include/cache/disk_aio"
  # cache
  "${PROJECT_SOURCE_DIR}/include/parallel" 
  # catalog
  "${PROJECT_SOURCE_DIR}/include/catalog"
  "${PROJECT_SOURCE_DIR}/include/catalog/default"
  "${PROJECT_SOURCE_DIR}/include/catalog/catalog_entry"
  # tbgpp-client
  "${PROJECT_SOURCE_DIR}/../tbgpp-client/include/main"
  # execution-engine
  "${PROJECT_SOURCE_DIR}/../tbgpp-execution-engine/include/execution"
  "${PROJECT_SOURCE_DIR}/../tbgpp-execution-engine/include/storage"
  # tbgpp-common
  "${PROJECT_SOURCE_DIR}/../tbgpp-common/include/common" "${PROJECT_SOURCE_DIR}/../tbgpp-common/include/common/enums"
  "${PROJECT_SOURCE_DIR}/../tbgpp-common/include/common/types" "${PROJECT_SOURCE_DIR}/../tbgpp-common/include/common/operator"
  "${PROJECT_SOURCE_DIR}/../tbgpp-common/include/common/value_operations" "${PROJECT_SOURCE_DIR}/../tbgpp-common/include/common/vector_operations"
)
target_link_libraries(tbgpp-graph-store-lib tbgpp-common-lib)
target_link_libraries(tbgpp-graph-store-lib tbgpp-main)
target_link_libraries(tbgpp-graph-store-lib "-pthread -ldl -lboost_system -lboost_timer -lboost_date_time -lboost_filesystem")
target_link_libraries(tbgpp-graph-store-lib MPI::MPI_CXX)



####################################################
# PER-TARGET DEFINITION - lightning
####################################################
set(CACHE_SRC_DIR "${PROJECT_SOURCE_DIR}/src/cache")
file (GLOB_RECURSE CACHE_SYSTEM_SOURCES "${CACHE_SRC_DIR}/*.c*")
SET(CACHE_LIB_SRC ${CACHE_SYSTEM_SOURCES})
target_sources(lightning PUBLIC ${CACHE_LIB_SRC})

target_link_libraries(lightning "-lrt -lpthread -ltbb -fopenmp -laio -lnuma -lhwloc")
target_link_libraries(lightning tbgpp-common-lib)

####################################################
# PER-TARGET DEFINITION - store
####################################################
target_sources(store PUBLIC "${PROJECT_SOURCE_DIR}/test/cache/test_store.cc")
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
	# tbgpp-common
  "${PROJECT_SOURCE_DIR}/../tbgpp-common/include"
  # common
	"${PROJECT_SOURCE_DIR}/../tbgpp-common/include/common" "${PROJECT_SOURCE_DIR}/../tbgpp-common/include/common/enums"
	"${PROJECT_SOURCE_DIR}/../tbgpp-common/include/common/types" "${PROJECT_SOURCE_DIR}/../tbgpp-common/include/common/operator"
	"${PROJECT_SOURCE_DIR}/../tbgpp-common/include/common/value_operations"
  #"${PROJECT_SOURCE_DIR}/../tbgpp-common/include/common/vector_operations"
  # parser
	"${PROJECT_SOURCE_DIR}/../include/parser"
	"${PROJECT_SOURCE_DIR}/../include/parser/parsed_data"
)
target_link_libraries(store lightning)
target_link_libraries(store tbgpp-common-lib)
target_link_libraries(store tbgpp-main)


####################################################
# TESTS
####################################################
# TODO need to link headers and other dependent libraries!!!

# file(GLOB CACHE_TEST_SRC
#   "${PROJECT_SOURCE_DIR}/test/cache/cache_test_single_thread.cc"
#   "${PROJECT_SOURCE_DIR}/test/cache/cache_test_multi_thread.cc"
# )

file(GLOB CATALOG_TEST_SRC
  #"${PROJECT_SOURCE_DIR}/test/catalog/catalog_test_single_thread.cc"
  #"${PROJECT_SOURCE_DIR}/test/catalog/catalog_test_shared_memory.cc"
  #"${PROJECT_SOURCE_DIR}/test/catalog/catalog_test_shared_memory2.cc"
  "${PROJECT_SOURCE_DIR}/test/catalog/catalog_test_catalog_server.cpp"
)

file(GLOB TILE_TEST_SRC
  #"${PROJECT_SOURCE_DIR}/test/extent/extent_test_json_reader.cc"
  #"${PROJECT_SOURCE_DIR}/test/extent/extent_test_csv_reader.cc"
  #"${PROJECT_SOURCE_DIR}/test/extent/extent_test_ldbc.cc"
  "${PROJECT_SOURCE_DIR}/test/schemaless/schemaless_simulation.cpp"
)

file(GLOB PARALLEL_TEST_SRC
  #"${PROJECT_SOURCE_DIR}/test/parallel/graph_partition_simulation.cpp"
  "${PROJECT_SOURCE_DIR}/test/parallel/graph_partition_simulation_new.cpp"
)

# if (FALSE)
# foreach(test_file ${CACHE_TEST_SRC})
#   get_filename_component(prog_name ${test_file} NAME_WE)
#   add_executable(${prog_name} "${CACHE_SRC_DIR}/chunk_cache_manager.cc" "${CACHE_SRC_DIR}/client.cc" ${test_file})
#   target_link_libraries(${prog_name} lightning Catch2::Catch2WithMain)
# endforeach(test_file ${CACHE_TEST_SRC})
# endif()

if (TRUE)
foreach(test_file ${CATALOG_TEST_SRC})
  get_filename_component(prog_name ${test_file} NAME_WE)
  add_executable(${prog_name} ${test_file})
  target_compile_options(${prog_name} PRIVATE ${CXX_FLAGS})
	target_compile_definitions(${prog_name} PRIVATE ${CXX_DEFINITION})
  target_link_libraries(${prog_name} PRIVATE tbgpp-graph-store-lib  Catch2::Catch2WithMain)
endforeach(test_file ${CATALOG_TEST_SRC})
endif()

foreach(test_file ${TILE_TEST_SRC})
  get_filename_component(prog_name ${test_file} NAME_WE)
  add_executable(${prog_name} ${test_file})
  target_compile_options(${prog_name} PRIVATE ${CXX_FLAGS})
  target_compile_definitions(${prog_name} PRIVATE ${CXX_DEFINITION})
  target_link_libraries(${prog_name} PRIVATE tbgpp-graph-store-lib Catch2::Catch2WithMain)
endforeach(test_file ${TILE_TEST_SRC})

foreach(test_file ${PARALLEL_TEST_SRC})
  set(CMAKE_CXX_COMPILER mpicxx)
  get_filename_component(prog_name ${test_file} NAME_WE)
  add_executable(${prog_name} ${test_file})
  target_compile_options(${prog_name} PRIVATE ${CXX_FLAGS}  -DTBB_ENABLE_IPO=OFF)
  target_compile_definitions(${prog_name} PRIVATE ${CXX_DEFINITION})
  target_link_libraries(${prog_name} PRIVATE tbgpp-graph-store-lib Catch2::Catch2WithMain -ltbb)
  target_link_libraries(${prog_name} PUBLIC MPI::MPI_CXX)
endforeach(test_file ${PARALLEL_TEST_SRC})
set(CMAKE_CXX_COMPILER g++)


#MESSAGE( STATUS "CMAKE_CXX_FLAGS: " ${CMAKE_CXX_FLAGS} )
