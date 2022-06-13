cmake_minimum_required(VERSION 3.10)

project(tbgpp-graph-store)

#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -lrt -lpthread -ltbb -fopenmp -laio -lnuma")
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -g")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3")

set(SRC_DIR "${PROJECT_SOURCE_DIR}/src")
#set(THIRD_PARTY_SRC_DIR "${PROJECT_SOURCE_DIR}/third_party")
set(CACHE_SRC_DIR "${PROJECT_SOURCE_DIR}/src/cache")
include_directories("${PROJECT_SOURCE_DIR}/include" "${PROJECT_SOURCE_DIR}/include/cache" 
                    "${PROJECT_SOURCE_DIR}/include/cache/disk_aio" "${PROJECT_SOURCE_DIR}/include/catalog"
                    "${PROJECT_SOURCE_DIR}/include/catalog/default" "${PROJECT_SOURCE_DIR}/include/catalog/catalog_entry"
                    "${PROJECT_SOURCE_DIR}/include/common" "${PROJECT_SOURCE_DIR}/include/common/enums"
                    "${PROJECT_SOURCE_DIR}/include/common/types" "${PROJECT_SOURCE_DIR}/include/common/operator"
                    "${PROJECT_SOURCE_DIR}/include/common/value_operations" "${PROJECT_SOURCE_DIR}/include/parser"
                    "${PROJECT_SOURCE_DIR}/include/parser/parsed_data" "${PROJECT_SOURCE_DIR}/include/main"
                    "${PROJECT_SOURCE_DIR}/include/third_party/fmt/include" "${PROJECT_SOURCE_DIR}/include/third_party/fast_float"
                    "${PROJECT_SOURCE_DIR}/include/third_party/utf8proc/include")

if(NOT CMAKE_BUILD_TYPE)
	set(CMAKE_BUILD_TYPE Release)
endif()

file (GLOB_RECURSE CACHE_SYSTEM_SOURCES "${CACHE_SRC_DIR}/*.c*")
SET(CACHE_LIB_SRC ${CACHE_SYSTEM_SOURCES})
file (GLOB_RECURSE SYSTEM_SOURCES "${SRC_DIR}/*.c*")
SET(TBGPP_SYSTEM_SOURCES ${SYSTEM_SOURCES})

add_library(lightning STATIC ${CACHE_LIB_SRC})
target_link_libraries(lightning "-lrt -lpthread -ltbb -fopenmp -laio -lnuma -lhwloc")

link_libraries(pthread aio numa rt hwloc tbb)

add_library(tbgpp-system-lib ${TBGPP_SYSTEM_SOURCES})
target_compile_options(tbgpp-system-lib PRIVATE ${CXX_FLAGS})
target_compile_definitions(tbgpp-system-lib PRIVATE ${CXX_DEFINITION})

#add_executable(store "${CACHE_SRC_DIR}/store.cc")
#target_link_libraries(store lightning)

file(GLOB CACHE_TEST_SRC
  "${PROJECT_SOURCE_DIR}/test/cache/cache_test_single_thread.cc"
  "${PROJECT_SOURCE_DIR}/test/cache/cache_test_multi_thread.cc"
)

file(GLOB CATALOG_TEST_SRC
  "${PROJECT_SOURCE_DIR}/test/catalog/catalog_test_single_thread.cc"
  "${PROJECT_SOURCE_DIR}/test/catalog/catalog_test_shared_memory.cc"
  "${PROJECT_SOURCE_DIR}/test/catalog/catalog_test_shared_memory2.cc"
  "${PROJECT_SOURCE_DIR}/test/catalog/catalog_test_catalog_server.cpp"
)

find_package(Catch2 REQUIRED)

foreach(test_file ${CACHE_TEST_SRC})
  get_filename_component(prog_name ${test_file} NAME_WE)
  add_executable(${prog_name} "${CACHE_SRC_DIR}/chunk_cache_manager.cc" "${CACHE_SRC_DIR}/client.cc" ${test_file})
  target_link_libraries(${prog_name} lightning Catch2::Catch2WithMain)
endforeach(test_file ${CACHE_TEST_SRC})

foreach(test_file ${CATALOG_TEST_SRC})
  get_filename_component(prog_name ${test_file} NAME_WE)
  add_executable(${prog_name} ${test_file})
  target_compile_options(${prog_name} PRIVATE ${CXX_FLAGS})
	target_compile_definitions(${prog_name} PRIVATE ${CXX_DEFINITION})
  target_link_libraries(${prog_name} PRIVATE tbgpp-system-lib Catch2::Catch2WithMain)
endforeach(test_file ${CATALOG_TEST_SRC})
