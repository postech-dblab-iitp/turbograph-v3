cmake_minimum_required(VERSION 3.10)

project(tbgpp-execution-engine)

set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_CXX_STANDARD 17)

set(SRC_DIR "${PROJECT_SOURCE_DIR}/src")

# dependencies - livegraph
add_library(livegraph SHARED IMPORTED)
set_property(TARGET livegraph PROPERTY IMPORTED_LOCATION "livegraph/liblivegraph.so")
target_include_directories(livegraph INTERFACE "${PROJECT_SOURCE_DIR}/livegraph/bind")
target_include_directories(livegraph INTERFACE "${PROJECT_SOURCE_DIR}/livegraph/core")

include_directories("${PROJECT_SOURCE_DIR}/livegraph/core")

# dependencies - tbb
include(cmake/FindTBB.cmake)
find_package(Threads REQUIRED)
link_libraries(Threads::Threads)

find_package(TBB REQUIRED)
include_directories(${TBB_INCLUDE_DIRS})
link_directories(${TBB_LIBRARY_DIRS})
link_libraries(${TBB_LIBRARIES})

# add system code
include_directories( "${SRC_DIR}" )
file(GLOB SOURCES
    ${SRC_DIR}/*.cpp
    ${SRC_DIR}/*/*.cpp
)
include_directories( "${PROJECT_SOURCE_DIR}/include" )

# set executable
add_executable( demo_main.out ${SOURCES} )

# link
target_link_libraries( demo_main.out PRIVATE livegraph )