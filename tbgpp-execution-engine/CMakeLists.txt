cmake_minimum_required(VERSION 3.10)

project(tbgpp-execution-engine)

set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_CXX_STANDARD 17)

set(SRC_DIR "${PROJECT_SOURCE_DIR}/src")

# dependencies - livegraph
#add_library(livegraph SHARED IMPORTED)
#set_property(TARGET livegraph PROPERTY IMPORTED_LOCATION "${PROJECT_SOURCE_DIR}/livegraph/liblivegraph.so")
#target_include_directories(livegraph INTERFACE "${PROJECT_SOURCE_DIR}/livegraph/bind")
#target_include_directories(livegraph INTERFACE "${PROJECT_SOURCE_DIR}/livegraph/core")

#include_directories("${PROJECT_SOURCE_DIR}/livegraph/core")

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake")
# dependencies - tbb
include(cmake/FindTBB.cmake)
find_package(Threads REQUIRED)
link_libraries(Threads::Threads)

find_package(TBB REQUIRED)
include_directories(${TBB_INCLUDE_DIRS})
link_directories(${TBB_LIBRARY_DIRS})
link_libraries(${TBB_LIBRARIES})

# dependencies - boost
set(Boost_DEBUG 1)
include(cmake/FindBoost.cmake)
find_package(Boost REQUIRED)
include_directories(${Boost_INCLUDE_DIRS})
link_directories(${Boost_LIBRARY_DIRS})
link_libraries(${Boost_LIBRARIES})

# dependencies - openmp
find_package(OpenMP REQUIRED)
if (OPENMP_FOUND)
	set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS}")
	set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
	set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${OpenMP_EXE_LINKER_FLAGS}")
endif()

# add system code
include_directories( "${SRC_DIR}" )
file (GLOB_RECURSE SOURCES "${SRC_DIR}/*.c*")

include_directories( "${PROJECT_SOURCE_DIR}/include" )
#include_directories( "${PROJECT_SOURCE_DIR}/include/duckdb/third_party/fmt/include" )
#include_directories( "${PROJECT_SOURCE_DIR}/include/duckdb/third_party/utf8proc/include")
#include_directories( "${PROJECT_SOURCE_DIR}/include/duckdb/third_party/miniz")
#include_directories( "${PROJECT_SOURCE_DIR}/include/duckdb/third_party/fast_float")
#include_directories( "${PROJECT_SOURCE_DIR}/include/duckdb/third_party/pcg")
#include_directories( "${PROJECT_SOURCE_DIR}/include/duckdb/third_party/re2")
#include_directories( "${PROJECT_SOURCE_DIR}/include/duckdb/third_party/hyperloglog")
#include_directories( "${PROJECT_SOURCE_DIR}/include/duckdb/third_party/concurrentqueue")
#include_directories( "${PROJECT_SOURCE_DIR}/include/duckdb/third_party/fastpforlib")
#include_directories( "${PROJECT_SOURCE_DIR}/include/duckdb/third_party/google/protobuf")
#include_directories( "${PROJECT_SOURCE_DIR}/include/duckdb/third_party/")
#include_directories( "${PROJECT_SOURCE_DIR}/include/duckdb/third_party/httplib")
#include_directories( "${PROJECT_SOURCE_DIR}/include/duckdb/third_party/picohash")
#include_directories( "${PROJECT_SOURCE_DIR}/include/duckdb/third_party/snappy")
#include_directories( "${PROJECT_SOURCE_DIR}/include/duckdb/third_party/snowball")
#include_directories( "${PROJECT_SOURCE_DIR}/include/duckdb/third_party/substrait")
#include_directories( "${PROJECT_SOURCE_DIR}/include/duckdb/third_party/tdigest")
# include_directories( "${PROJECT_SOURCE_DIR}/include/duckdb/third_party/tpce-tool/include/")
# include_directories( "${PROJECT_SOURCE_DIR}/include/duckdb/third_party/tpce-tool/include/utilities")
# include_directories( "${PROJECT_SOURCE_DIR}/include/duckdb/third_party/tpce-tool/include/input")
# include_directories( "${PROJECT_SOURCE_DIR}/include/duckdb/third_party/tpce-tool/include/main")
# include_directories( "${PROJECT_SOURCE_DIR}/include/duckdb/third_party/tpce-tool/include/main/win")

#include_directories( "${PROJECT_SOURCE_DIR}/include/duckdb/third_party/zstd")
#include_directories( "${PROJECT_SOURCE_DIR}/include/duckdb/third_party/zstd/include")
#include_directories( "${PROJECT_SOURCE_DIR}/include/duckdb/third_party/zstd/include/zstd/common")
#include_directories( "${PROJECT_SOURCE_DIR}/include/duckdb/third_party/zstd/include/zstd/compress")
#include_directories( "${PROJECT_SOURCE_DIR}/include/duckdb/third_party/zstd/include/zstd/decompress")
#include_directories( "${PROJECT_SOURCE_DIR}/include/duckdb/third_party/catch")
#include_directories( "${PROJECT_SOURCE_DIR}/include/duckdb/third_party/imdb")
#include_directories( "${PROJECT_SOURCE_DIR}/include/duckdb/third_party/imdb/include")
# include_directories( "${PROJECT_SOURCE_DIR}/include/duckdb/third_party/libpg_query")
# include_directories( "${PROJECT_SOURCE_DIR}/include/duckdb/third_party/libpg_query/grammar")
# include_directories( "${PROJECT_SOURCE_DIR}/include/duckdb/third_party/libpg_query/include")
# include_directories( "${PROJECT_SOURCE_DIR}/include/duckdb/third_party/libpg_query/include/nodes")
# include_directories( "${PROJECT_SOURCE_DIR}/include/duckdb/third_party/libpg_query/include/access")
# include_directories( "${PROJECT_SOURCE_DIR}/include/duckdb/third_party/libpg_query/include/catalog")
# include_directories( "${PROJECT_SOURCE_DIR}/include/duckdb/third_party/libpg_query/include/common")
# include_directories( "${PROJECT_SOURCE_DIR}/include/duckdb/third_party/libpg_query/include/datatype")
# include_directories( "${PROJECT_SOURCE_DIR}/include/duckdb/third_party/libpg_query/include/mb")
# include_directories( "${PROJECT_SOURCE_DIR}/include/duckdb/third_party/libpg_query/include/parser")
#include_directories( "${PROJECT_SOURCE_DIR}/include/duckdb/third_party/parquet")
#include_directories( "${PROJECT_SOURCE_DIR}/include/duckdb/third_party/sqlite")
#include_directories( "${PROJECT_SOURCE_DIR}/include/duckdb/third_party/thrift")


add_definitions(-DDUCKDB_SOURCE_ID="\""1"\"")
add_definitions(-DDUCKDB_VERSION="\""1"\"")

file(GLOB ENGINE_TEST_SRC
  "${PROJECT_SOURCE_DIR}/test/demo_main.cpp"
)

foreach(test_file ${ENGINE_TEST_SRC})
  get_filename_component(prog_name ${test_file} NAME_WE)
  add_executable(${prog_name} ${test_file} ${SOURCES} )
  target_compile_options(${prog_name} PRIVATE ${CXX_FLAGS})
	target_compile_definitions(${prog_name} PRIVATE ${CXX_DEFINITION})
  target_link_libraries(${prog_name} PRIVATE tbgpp-system-lib "-ldl -pthread" )
endforeach(test_file ${TILE_TEST_SRC})

# set executable
#add_executable( demo_main.out ${SOURCES} )
#add_executable( livegraph-test tmp/_insert_graph.cpp )

# link
#target_link_libraries( demo_main.out PRIVATE tbgpp-system-lib "-ldl -pthread" )
#target_link_libraries( livegraph-test PRIVATE livegraph "-ldl -pthread")
