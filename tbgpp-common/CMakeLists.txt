cmake_minimum_required(VERSION 3.10)

project(tbgpp-common)

set(CMAKE_CXX_FLAGS "-std=c++17")

add_library(tbgpp-system-common INTERFACE)
target_include_directories(tbgpp-system-common INTERFACE "${PROJECT_SOURCE_DIR}/icecream-cpp/include/")