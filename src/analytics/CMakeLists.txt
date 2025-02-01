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
# target_include_directories(tbgpp-analytics PUBLIC
# 	"${PROJECT_SOURCE_DIR}/include"
# 	"${PROJECT_SOURCE_DIR}/include/util"
# 	"${PROJECT_SOURCE_DIR}/include/io"
# 	"${PROJECT_SOURCE_DIR}/include/datastructure"
# 	"${PROJECT_SOURCE_DIR}/include/core"
# )

file (GLOB_RECURSE SYSTEM_SOURCES 
	"${PROJECT_SOURCE_DIR}/src/*.c*"
)

SET(TBGPP_SYSTEM_SOURCES ${SYSTEM_SOURCES})
target_sources(tbgpp-analytics PUBLIC ${TBGPP_SYSTEM_SOURCES})

target_link_libraries(tbgpp-analytics "-lrt -lpthread -ltbb -fopenmp -laio -lnuma -lhwloc")
target_link_libraries(tbgpp-analytics common-lib)
target_compile_options(tbgpp-analytics PUBLIC ${CXX_FLAGS})
target_compile_definitions(tbgpp-analytics PUBLIC ${CXX_DEFINITION})

# set(COMMON_DEFINITION -DUSE_DIRECT_TCP=1 -DCHECK_INVARIANTS -DUSE_HWLOC -DOldMessageTransfer ${PLATFORM})

# set(DEBUG_DEFINITION -DDEBUG)
# set(RELEASE_DEFINITION -DNDEBUG -DPERFORMANCE)

# if(CMAKE_BUILD_TYPE STREQUAL Release)
# 	set(CXX_DEFINITION ${CXX_DEFINITION} ${COMMON_DEFINITION} ${RELEASE_DEFINITION})
# else()
# 	set(CXX_DEFINITION ${CXX_DEFINITION} ${COMMON_DEFINITION} ${DEBUG_DEFINITION})
# endif()


# 6. Set CXX flags
# set(CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS} -fopenmp)
# add_compile_options(-std=c++17 -D_GLIBCXX_USE_CXX11_ABI=0)
# add_compile_options(-march=native -msse -mavx -mavx2 -mbmi2 -mlzcnt -rdynamic -Wformat=0 -Wl,-rpath,${LIBRARY_DIR})

# 4. Link directories and libraries
# if(CMAKE_BUILD_TYPE STREQUAL Release)
# 	set(LIBRARY_DIR ${LIBRARY_DIR} "${CMAKE_CURRENT_SOURCE_DIR}/lib/release")
# 	set(LIBRARY_DIR ${LIBRARY_DIR} ${TBB_LIB_RELEASE_PATH})
# 	set(LIBRARY_DIR ${LIBRARY_DIR} ${Jemalloc_LIB_RELEASE_PATH})
# 	set(LIBRARY_DIR ${LIBRARY_DIR} ${HWLOC_LIB_RELEASE_PATH})
# 	set(LIBRARY_DIR ${LIBRARY_DIR} ${LIBAIO_LIB_RELEASE_PATH})
# 	set(LIBRARY_DIR ${LIBRARY_DIR} ${OPENMP_LIB_RELEASE_PATH})
# else()
# 	set(LIBRARY_DIR ${LIBRARY_DIR} "${CMAKE_CURRENT_SOURCE_DIR}/lib/debug")
# 	set(LIBRARY_DIR ${LIBRARY_DIR} ${TBB_LIB_DEBUG_PATH})
# 	set(LIBRARY_DIR ${LIBRARY_DIR} ${Jemalloc_LIB_DEBUG_PATH})
# 	set(LIBRARY_DIR ${LIBRARY_DIR} ${HWLOC_LIB_DEBUG_PATH})
# 	set(LIBRARY_DIR ${LIBRARY_DIR} ${LIBAIO_LIB_DEBUG_PATH})
# 	set(LIBRARY_DIR ${LIBRARY_DIR} ${OPENMP_LIB_DEBUG_PATH})
# endif()

# link_directories(${LIBRARY_DIR})
# link_libraries(pthread glog jemalloc aio numa rt hwloc tbb)

# 5. Set definitions

# execute_process(
#   COMMAND git rev-parse HEAD
#   WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
#   OUTPUT_VARIABLE VERSION
#   OUTPUT_STRIP_TRAILING_WHITESPACE
# )
# set (CXX_DEFINITION ${CXX_DEFINITION} -DVERSION=\"${VERSION}\")


# if(CMAKE_CXX_COMPILER STREQUAL mpiicpc)
# 	add_compile_options(-mt_mpi)
# endif()

# if(CMAKE_BUILD_TYPE STREQUAL Release)
# 	add_compile_options(-g -Wstrict-aliasing=0)
# else()
# 	add_compile_options(-g)
# endif()

# 7. Get source code files
# file (GLOB_RECURSE SYSTEM_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/src/*.c*")
# SET(TBGPP_SYSTEM_SOURCES ${SYSTEM_SOURCES})

# 8. Compile the tbgpp-system and add it as a library
# add_library(tbgpp-system-lib ${TBGPP_SYSTEM_SOURCES})
# target_compile_definitions(tbgpp-system-lib PRIVATE ${CXX_DEFINITION})

add_executable(pr_int generated_queries/pr_int.cpp)
target_compile_options(pr_int PRIVATE ${})
target_compile_definitions(pr_int PRIVATE ${CXX_DEFINITION})
target_link_libraries(pr_int PRIVATE tbgpp-analytics)

# add_subdirectory(generated_queries)
# add_subdirectory(test)
