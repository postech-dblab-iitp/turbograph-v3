cmake_minimum_required(VERSION 3.16)

####################################################
# PROJECT METADATA 
####################################################
project(tbgpp-c-api)

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
add_library(tbgpp-c-api-shared SHARED)
add_library(tbgpp-c-api STATIC)

####################################################
# GLOBAL DEFINITION
####################################################

####################################################
# PER-TARGET DEFINITION - tbgpp-server
####################################################
target_include_directories(tbgpp-c-api PUBLIC
	"${PROJECT_SOURCE_DIR}/../common/include"
)
target_include_directories(tbgpp-c-api-shared PUBLIC
	"${PROJECT_SOURCE_DIR}/../common/include"
)
file (GLOB_RECURSE SYSTEM_SOURCES "${PROJECT_SOURCE_DIR}/src/*.c*")
target_sources(tbgpp-c-api PUBLIC ${SYSTEM_SOURCES})
target_sources(tbgpp-c-api-shared PUBLIC ${SYSTEM_SOURCES})
target_link_libraries(tbgpp-c-api common-lib)
target_link_libraries(tbgpp-c-api storage-lib)
target_link_libraries(tbgpp-c-api execution-lib)
target_link_libraries(tbgpp-c-api nlohmann_json::nlohmann_json)
target_link_libraries(tbgpp-c-api "-lrt -pthread -ldl -lboost_system -lboost_timer -lboost_date_time -lboost_filesystem -ltbb -fopenmp -laio -lnuma -lhwloc")
target_link_libraries(tbgpp-c-api-shared common-lib)
target_link_libraries(tbgpp-c-api-shared storage-lib)
target_link_libraries(tbgpp-c-api-shared nlohmann_json::nlohmann_json)
target_link_libraries(tbgpp-c-api-shared "-lrt -pthread -ldl -lboost_system -lboost_timer -lboost_date_time -lboost_filesystem -ltbb -fopenmp -laio -lnuma -lhwloc")

target_compile_options(tbgpp-c-api PUBLIC ${CXX_FLAGS})
target_compile_definitions(tbgpp-c-api PUBLIC ${CXX_DEFINITION})
target_compile_options(tbgpp-c-api-shared PUBLIC ${CXX_FLAGS})
target_compile_definitions(tbgpp-c-api-shared PUBLIC ${CXX_DEFINITION})

####################################################
# TESTS
####################################################
file(GLOB CLIENT_TEST_SRC
  "${PROJECT_SOURCE_DIR}/test/s62-c-api-test.c"
  "${PROJECT_SOURCE_DIR}/test/recommendation-system.c"
)

foreach(test_file ${CLIENT_TEST_SRC})
  get_filename_component(prog_name ${test_file} NAME_WE)
  add_executable(${prog_name} ${test_file} )
  target_compile_options(${prog_name} PRIVATE ${C_FLAGS})
  target_compile_definitions(${prog_name} PRIVATE ${C_DEFINITION})
  target_link_libraries(${prog_name} tbgpp-c-api)
endforeach(test_file ${CLIENT_TEST_SRC})
