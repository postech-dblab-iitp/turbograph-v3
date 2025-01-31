cmake_minimum_required(VERSION 3.10)

# Project name
project(CuckooFilter)

# Source files for the library
set(LIB_SOURCES
    ${PROJECT_SOURCE_DIR}/src/hashutil.cc
)

# Add library target
add_library(cuckoofilter STATIC ${LIB_SOURCES})

# Include directories for the target
target_include_directories(cuckoofilter PUBLIC ${PROJECT_SOURCE_DIR}/include ${PROJECT_SOURCE_DIR}/src)