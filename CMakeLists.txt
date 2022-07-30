cmake_minimum_required(VERSION 3.16)

# SET CPP version 
set(CMAKE_C_COMPILER gcc)
set(CMAKE_CXX_COMPILER g++)
set(CMAKE_CXX_FLAGS                  "-std=c++17 -march=native -mavx2")
set (CMAKE_CXX_STANDARD 17)

project(turbograph-v3)

add_subdirectory(tbgpp-graph-store)
add_subdirectory(tbgpp-execution-engine)
