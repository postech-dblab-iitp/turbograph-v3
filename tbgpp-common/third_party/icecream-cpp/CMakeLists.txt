cmake_minimum_required(VERSION 3.16)

project(Icecream-test)

find_package(Boost 1.65.0 REQUIRED)

#enable_testing()

set(CMAKE_CXX_STANDARD 17)
add_compile_definitions(CPP_17)

set(CMAKE_BUILD_TYPE Debug)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

include_directories(${Boost_INCLUDE_DIRS})

# add_executable(test_exec test.cpp test_2.cpp)
# add_test(NAME test COMMAND test_exec)
