cmake_minimum_required(VERSION 3.16)

####################################################
# PROJECT METADATA 
####################################################
project(analytics)

####################################################
# DECLARE TARGETS
####################################################
add_library(tbgpp-analytics STATIC)

####################################################
# PER-TARGET DEFINITION - tbgpp-analytics
####################################################
target_include_directories(tbgpp-analytics PUBLIC
	"${PROJECT_SOURCE_DIR}/include"
	"${PROJECT_SOURCE_DIR}/include/util"
	"${PROJECT_SOURCE_DIR}/include/io"
	"${PROJECT_SOURCE_DIR}/include/datastructure"
	"${PROJECT_SOURCE_DIR}/include/core"
)

SET(PLATFORM -DLINUX)

# if(NOT CMAKE_BUILD_TYPE)
# 	set(CMAKE_BUILD_TYPE Release)
# endif()

# 2. Get paths to installed libraries
# 2-1. Get a path to TBB 
# if (NOT TBB_INCLUDE_PATH)
# 	message(WARNING "No configured TBB")
# 	set(TBB_INCLUDE_PATH /tbgpp-libs/oneTBB-2018_U5/include)
# 	set(TBB_LIB_RELEASE_PATH /tbgpp-libs/oneTBB-2018_U5/build/linux_intel64_gcc_cc8_libc2.31_kernel4.15.0_release)
# 	set(TBB_LIB_DEBUG_PATH /tbgpp-libs/oneTBB-2018_U5/build/linux_intel64_gcc_cc8_libc2.31_kernel4.15.0_debug)
# endif()

# if (NOT EXISTS ${TBB_INCLUDE_PATH})
# 	message(FATAL_ERROR "Invalid TBB")
# 	return()
# endif()

# message("TBB PATHs")
# message("  ${TBB_INCLUDE_PATH}")
# message("  ${TBB_LIB_RELEASE_PATH}")
# message("  ${TBB_LIB_DEBUG_PATH}")

# 2-2. Get a path to Jemalloc 
# if (NOT Jemalloc_INCLUDE_PATH)
# 	message(WARNING "No configured Jemalloc")
# 	set(Jemalloc_INCLUDE_PATH /tbgpp-libs/jemalloc/include)
# 	set(Jemalloc_LIB_RELEASE_PATH /tbgpp-libs/jemalloc/lib)
# 	set(Jemalloc_LIB_DEBUG_PATH /tbgpp-libs/jemalloc/lib)
# endif()
# if (NOT EXISTS ${Jemalloc_INCLUDE_PATH})
# 	message(FATAL_ERROR "Invalid Jemalloc")
# 	return()
# endif()
# message("Jemalloc PATHs")
# message("  ${Jemalloc_INCLUDE_PATH}")
# message("  ${Jemalloc_LIB_RELEASE_PATH}")
# message("  ${Jemalloc_LIB_DEBUG_PATH}")

# # 2-3. Get a path to hwloc
# if (NOT HWLOC_INCLUDE_PATH)
# 	message(WARNING "No configured hwloc")
# 	set(HWLOC_INCLUDE_PATH /usr/local/include)
# 	set(HWLOC_LIB_RELEASE_PATH /usr/local/lib)
# 	set(HWLOC_LIB_DEBUG_PATH /usr/local/lib)
# endif()
# if (NOT EXISTS ${HWLOC_INCLUDE_PATH}/hwloc.h)
# 	message(FATAL_ERROR "Invalid hwloc")
# 	return()
# endif()
# message("HWLOC PATHs")
# message("  ${HWLOC_INCLUDE_PATH}")
# message("  ${HWLOC_LIB_RELEASE_PATH}")
# message("  ${HWLOC_LIB_DEBUG_PATH}")

# # 2-4. Get a path to libaio
# if (NOT LIBAIO_INCLUDE_PATH)
# 	message(WARNING "No configured libaio")
# 	set(LIBAIO_INCLUDE_PATH /usr/include)
# 	set(LIBAIO_LIB_RELEASE_PATH /usr/lib)
# 	set(LIBAIO_LIB_DEBUG_PATH /usr/lib)
# endif()
# if (NOT EXISTS ${LIBAIO_INCLUDE_PATH}/libaio.h)
# 	message(FATAL_ERROR "Invalid libaio")
# 	return()
# endif()
# message("LIBAIO PATHs")
# message("  ${LIBAIO_INCLUDE_PATH}")
# message("  ${LIBAIO_LIB_RELEASE_PATH}")
# message("  ${LIBAIO_LIB_DEBUG_PATH}")

# # 2-4. Get a path to openmp
# if (NOT OPENMP_INCLUDE_PATH)
# 	message(WARNING "No configured openmp")
# 	set(OPENMP_INCLUDE_PATH /usr/local/include)
# 	set(OPENMP_LIB_RELEASE_PATH /usr/local/lib)
# 	set(OPENMP_LIB_DEBUG_PATH /usr/local/lib)
# endif()
# if (NOT EXISTS ${OPENMP_INCLUDE_PATH}/omp.h)
# 	message(FATAL_ERROR "Invalid openmp")
# 	return()
# endif()
# #set (CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS} "-fopenmp")
# #set (CMAKE_C_FLAGS ${CMAKE_C_FLAGS} "-fopenmp")
# message("OPENMP PATHs")
# message("  ${OPENMP_INCLUDE_PATH}")
# message("  ${OPENMP_LIB_RELEASE_PATH}")
# message("  ${OPENMP_LIB_DEBUG_PATH}")



# 3. Set include directories
include_directories(
	${CMAKE_CURRENT_SOURCE_DIR}/include 
	${CMAKE_CURRENT_SOURCE_DIR}/include/util 
	${CMAKE_CURRENT_SOURCE_DIR}/include/io 
	${CMAKE_CURRENT_SOURCE_DIR}/include/datastructure 
	${CMAKE_CURRENT_SOURCE_DIR}/include/core 
	${CMAKE_CURRENT_SOURCE_DIR}/include/io/disk_aio
	${TBB_INCLUDE_PATH}
	${Jemalloc_INCLUDE_PATH}
	${HWLOC_INCLUDE_PATH}
	${LIBAIO_INCLUDE_PATH}
	${OPENMP_INCLUDE_PATH}
	)

# 4. Link directories and libraries
if(CMAKE_BUILD_TYPE STREQUAL Release)
	set(LIBRARY_DIR ${LIBRARY_DIR} "${CMAKE_CURRENT_SOURCE_DIR}/lib/release")
	set(LIBRARY_DIR ${LIBRARY_DIR} ${TBB_LIB_RELEASE_PATH})
	set(LIBRARY_DIR ${LIBRARY_DIR} ${Jemalloc_LIB_RELEASE_PATH})
	set(LIBRARY_DIR ${LIBRARY_DIR} ${HWLOC_LIB_RELEASE_PATH})
	set(LIBRARY_DIR ${LIBRARY_DIR} ${LIBAIO_LIB_RELEASE_PATH})
	set(LIBRARY_DIR ${LIBRARY_DIR} ${OPENMP_LIB_RELEASE_PATH})
else()
	set(LIBRARY_DIR ${LIBRARY_DIR} "${CMAKE_CURRENT_SOURCE_DIR}/lib/debug")
	set(LIBRARY_DIR ${LIBRARY_DIR} ${TBB_LIB_DEBUG_PATH})
	set(LIBRARY_DIR ${LIBRARY_DIR} ${Jemalloc_LIB_DEBUG_PATH})
	set(LIBRARY_DIR ${LIBRARY_DIR} ${HWLOC_LIB_DEBUG_PATH})
	set(LIBRARY_DIR ${LIBRARY_DIR} ${LIBAIO_LIB_DEBUG_PATH})
	set(LIBRARY_DIR ${LIBRARY_DIR} ${OPENMP_LIB_DEBUG_PATH})
endif()

link_directories(${LIBRARY_DIR})
link_libraries(pthread glog jemalloc aio numa rt hwloc tbb)

# 5. Set definitions

# execute_process(
#   COMMAND git rev-parse HEAD
#   WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
#   OUTPUT_VARIABLE VERSION
#   OUTPUT_STRIP_TRAILING_WHITESPACE
# )
# set (CXX_DEFINITION ${CXX_DEFINITION} -DVERSION=\"${VERSION}\")

set(COMMON_DEFINITION -DUSE_DIRECT_TCP=1 -DCHECK_INVARIANTS -DUSE_HWLOC -DOldMessageTransfer ${PLATFORM})

set(DEBUG_DEFINITION -DDEBUG)
set(RELEASE_DEFINITION -DNDEBUG -DPERFORMANCE)

if(CMAKE_BUILD_TYPE STREQUAL Release)
	set(CXX_DEFINITION ${CXX_DEFINITION} ${COMMON_DEFINITION} ${RELEASE_DEFINITION})
else()
	set(CXX_DEFINITION ${CXX_DEFINITION} ${COMMON_DEFINITION} ${DEBUG_DEFINITION})
endif()

# message("CXX_DEFINITIONS: ${CXX_DEFINITION}")
#set(CMAKE_CXX_DEFINITION ${CMAKE_CXX_DEFINITION} ${CXX_DEFINITION})
#add_definitions(${CXX_DEFINITIONS})


# 6. Set CXX flags
set(CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS} -fopenmp)
add_compile_options(-std=c++11 -D_GLIBCXX_USE_CXX11_ABI=0)
add_compile_options(-march=native -msse -mavx -mavx2 -mbmi2 -mlzcnt -rdynamic -Wformat=0 -Wl,-rpath,${LIBRARY_DIR})

if(CMAKE_CXX_COMPILER STREQUAL mpiicpc)
	add_compile_options(-mt_mpi)
endif()

if(CMAKE_BUILD_TYPE STREQUAL Release)
	add_compile_options(-g -Wstrict-aliasing=0)
else()
	add_compile_options(-g)
endif()

if(PROFILE)
	add_compile_options(-pg)
endif()

# 7. Get source code files
file (GLOB_RECURSE SYSTEM_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/src/*.c*")
SET(TBGPP_SYSTEM_SOURCES ${SYSTEM_SOURCES})

# 8. Compile the tbgpp-system and add it as a library
add_library(tbgpp-system-lib ${TBGPP_SYSTEM_SOURCES})
target_compile_definitions(tbgpp-system-lib PRIVATE ${CXX_DEFINITION})

#add_executable(buildDDB applications/buildDDB.cpp)
#target_compile_options(buildDDB PRIVATE ${})
#target_compile_definitions(buildDDB PRIVATE ${CXX_DEFINITION})
#target_link_libraries(buildDDB PRIVATE tbgpp-system-lib)

add_subdirectory(generated_queries)
add_subdirectory(test)
