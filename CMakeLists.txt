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
project(s62)

#######################
### Set environment ###
#######################
set(CMAKE_CXX_FLAGS "-std=c++17 -march=native -mavx2 -mavx -fPIE -DTBB_ENABLE_IPO=OFF -D__TBB_DYNAMIC_LOAD_ENABLED=0 -DCMAKE_PREFIX_PATH=/opt/intel/oneapi/tbb/latest")
# set S62 Debug type and Orca Debug type same
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
	SET(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DGPOS_DEBUG")
    # Compilation flags for sanitizers
    # set(SANITIZER_FLAGS "-fsanitize=address,undefined")
    # set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${SANITIZER_FLAGS}")
    # set(CMAKE_LINKER_FLAGS "${CMAKE_LINKER_FLAGS} ${SANITIZER_FLAGS}")
    # set(ENV{ASAN_OPTIONS} "verbosity=1")
endif()
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

##########################
### Add subdirectories ###
##########################
add_subdirectory(s62-common)
add_subdirectory(s62-graph-store)
add_subdirectory(s62-execution-engine)
add_subdirectory(s62-compiler)
add_subdirectory(s62-client)
add_subdirectory(s62-api-server)

####################
### Set compiler ###
####################
set(CMAKE_C_COMPILER gcc) 
set(CMAKE_CXX_COMPILER g++)

####################
### Set launcher ###
####################
find_program(CCACHE "ccache")
if(CCACHE)
    set(CMAKE_C_COMPILER_LAUNCHER ${CCACHE})
    set(CMAKE_CXX_COMPILER_LAUNCHER ${CCACHE})
endif(CCACHE)

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