cmake_minimum_required(VERSION 3.16)

####################################################
# PROJECT METADATA 
####################################################
project(s62-api-server)

####################################################
# ADD SUBDIRECTORIES
####################################################

####################################################
# COMPILATION FLAGS
####################################################
set(CMAKE_CXX_STANDARD 17)
if(NOT CMAKE_BUILD_TYPE)
	set(CMAKE_BUILD_TYPE Release)
endif()

####################################################
# DECLARE TARGETS
####################################################
add_library(s62-api-server-shared SHARED)
add_library(s62-api-server STATIC)

####################################################
# GLOBAL DEFINITION
####################################################

####################################################
# PER-TARGET DEFINITION - s62-server
####################################################
target_include_directories(s62-api-server PUBLIC
	"${PROJECT_SOURCE_DIR}/include"
	"${PROJECT_SOURCE_DIR}/../s62-common/include"
)
target_include_directories(s62-api-server-shared PUBLIC
	"${PROJECT_SOURCE_DIR}/include"
	"${PROJECT_SOURCE_DIR}/../s62-common/include"
)
file (GLOB_RECURSE SYSTEM_SOURCES "${PROJECT_SOURCE_DIR}/src/*.c*")
target_sources(s62-api-server PUBLIC ${SYSTEM_SOURCES})
target_sources(s62-api-server-shared PUBLIC ${SYSTEM_SOURCES})
target_link_libraries(s62-api-server s62-common-lib)
target_link_libraries(s62-api-server s62-graph-store-lib)
target_link_libraries(s62-api-server s62-execution-engine-lib)
target_link_libraries(s62-api-server nlohmann_json::nlohmann_json)
target_link_libraries(s62-api-server "-lrt -pthread -ldl -lboost_system -lboost_timer -lboost_date_time -lboost_filesystem -ltbb -fopenmp -laio -lnuma -lhwloc")
target_link_libraries(s62-api-server-shared s62-common-lib)
target_link_libraries(s62-api-server-shared s62-graph-store-lib)
target_link_libraries(s62-api-server-shared nlohmann_json::nlohmann_json)
target_link_libraries(s62-api-server-shared "-lrt -pthread -ldl -lboost_system -lboost_timer -lboost_date_time -lboost_filesystem -ltbb -fopenmp -laio -lnuma -lhwloc")

target_compile_options(s62-api-server PUBLIC ${CXX_FLAGS})
target_compile_definitions(s62-api-server PUBLIC ${CXX_DEFINITION})
target_compile_options(s62-api-server-shared PUBLIC ${CXX_FLAGS})
target_compile_definitions(s62-api-server-shared PUBLIC ${CXX_DEFINITION})

####################################################
# TESTS
####################################################
file(GLOB CLIENT_TEST_SRC
  # "${PROJECT_SOURCE_DIR}/test/s62-c-api-test.c"
)

foreach(test_file ${CLIENT_TEST_SRC})
  get_filename_component(prog_name ${test_file} NAME_WE)
  add_executable(${prog_name} ${test_file} )
  target_compile_options(${prog_name} PRIVATE ${C_FLAGS})
  target_compile_definitions(${prog_name} PRIVATE ${C_DEFINITION})
  target_link_libraries(${prog_name} s62-api-server)
endforeach(test_file ${CLIENT_TEST_SRC})
