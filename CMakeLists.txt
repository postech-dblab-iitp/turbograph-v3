cmake_minimum_required(VERSION 3.16)

# Section headers in this CMakeLists file
# - Project metadata
# - Set environment
# - Add subdirectories
# - Set compiler
# - Target > unittest
# - Target > format
# - Target > check-format

########################
### Project metadata ###
########################
project(turbograph-v3)

#######################
### Set environment ###
#######################
set(CMAKE_CXX_FLAGS "-std=c++17 -march=native -mavx2")
# set S62 Debug type and Orca Debug type same
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
	SET(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DGPOS_DEBUG")
endif()
set(CMAKE_CXX_STANDARD 17)

##########################
### Add subdirectories ###
##########################
add_subdirectory(tbgpp-common)
add_subdirectory(tbgpp-graph-store)
add_subdirectory(tbgpp-execution-engine)
add_subdirectory(tbgpp-compiler)
add_subdirectory(tbgpp-client)

####################
### Set compiler ###
####################
set(CMAKE_C_COMPILER gcc) 
set(CMAKE_CXX_COMPILER g++)

#########################
### Target > unittest ###
#########################
if( 0 )	# TODO Add option
else()
	add_custom_target(unittest
		echo "unittest"
		USES_TERMINAL
	)
endif()

#######################
### Target > format ###
#######################
if( 0 )	# TODO Add option
else()
	add_custom_target(format
		echo "format"
		USES_TERMINAL
	)
endif()

#############################
### Target > check-format ###
#############################
if( 0 )	# TODO Add option
else()
	add_custom_target(check-format
		echo "check-format"
		USES_TERMINAL
	)
endif()

###########################
### Target > check-lint ###
###########################
# TODO target format overlaps
if( 0 )	# TODO Add option
else()
	add_custom_target(formatttttttt
		echo "format"
		USES_TERMINAL
	)
endif()
