cmake_minimum_required(VERSION 3.16)
project(analytics)

find_package(OpenMP REQUIRED)
set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS}")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${OpenMP_EXE_LINKER_FLAGS}")

find_package(TBB REQUIRED)
set(TBB_DEFINITIONS "-DTBB_ENABLE_IPO=OFF -D__TBB_DYNAMIC_LOAD_ENABLED=0")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${TBB_DEFINITIONS}")
set(CMAKE_PREFIX_PATH "/opt/intel/oneapi/tbb/latest")
include_directories(${TBB_INCLUDE_DIRS})
link_directories(${TBB_LIBRARY_DIRS})
link_libraries(${TBB_LIBRARIES})

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mpopcnt")

add_library(${ANALYTICS_LIB_NAME} SHARED)
file (GLOB_RECURSE SOURCES "${PROJECT_SOURCE_DIR}/*.c*")
SET(TBGPP_SOURCES ${SOURCES})
target_sources(${ANALYTICS_LIB_NAME} PUBLIC ${TBGPP_SOURCES})
target_include_directories(${ANALYTICS_LIB_NAME} PUBLIC ${INCLUDE_DIR})

SET(TBGPP_LIBS
	rt
	pthread
	tbb
	aio
	numa
	hwloc
)

target_link_libraries(${ANALYTICS_LIB_NAME} ${TBGPP_LIBS})
target_compile_options(${ANALYTICS_LIB_NAME} PRIVATE -Wno-format)