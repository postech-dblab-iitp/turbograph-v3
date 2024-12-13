cmake_minimum_required(VERSION 3.16)

####################################################
# PROJECT METADATA 
####################################################
project(tbgpp-api)

####################################################
# ADD SUBDIRECTORIES
####################################################
#SET(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DGPOS_DEBUG")

add_subdirectory(c-api)
add_subdirectory(server)

####################
### Set compiler ###
####################
set(CMAKE_C_COMPILER gcc) 
set(CMAKE_CXX_COMPILER g++)