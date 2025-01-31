cmake_minimum_required(VERSION 3.16)

####################################################
# PROJECT METADATA 
####################################################
project(tbgpp-server)

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
add_library(tbgpp-server STATIC)

####################################################
# GLOBAL DEFINITION
####################################################

####################################################
# PER-TARGET DEFINITION - tbgpp-server
####################################################R}/../../common/include/common/vector_operations"
file (GLOB_RECURSE SYSTEM_SOURCES "${PROJECT_SOURCE_DIR}/src/*.c*")
target_sources(tbgpp-server PUBLIC ${SYSTEM_SOURCES})
target_link_libraries(tbgpp-server "-lrt -lpthread -ltbb -fopenmp -laio -lnuma -lhwloc")
target_link_libraries(tbgpp-server common-lib)
target_link_libraries(tbgpp-server storage-lib)
target_link_libraries(tbgpp-server execution-lib)
target_link_libraries(tbgpp-server compiler)

target_compile_options(tbgpp-server PUBLIC ${CXX_FLAGS})
target_compile_definitions(tbgpp-server PUBLIC ${CXX_DEFINITION})

####################################################
# TESTS
####################################################
file(GLOB CLIENT_TEST_SRC
   "${PROJECT_SOURCE_DIR}/test/socket_client_test.cpp"
   "${PROJECT_SOURCE_DIR}/test/socket_server_run.cpp"
)

foreach(test_file ${CLIENT_TEST_SRC})
  get_filename_component(prog_name ${test_file} NAME_WE)
  add_executable(${prog_name} ${test_file} )
  target_sources( ${prog_name} PUBLIC ${SOURCES} )
  target_sources( ${prog_name} PUBLIC ${TEST_SOURCES}  ) 
  target_compile_options(${prog_name} PRIVATE ${CXX_FLAGS})
  target_compile_definitions(${prog_name} PRIVATE ${CXX_DEFINITION})
  target_link_libraries(${prog_name} PRIVATE
    tbgpp-server
    common-lib
    storage-lib
    execution-lib
    nlohmann_json::nlohmann_json "-pthread -ldl -lboost_system -lboost_timer -lboost_date_time -lboost_filesystem -ltbb"
  )
endforeach(test_file ${CLIENT_TEST_SRC})
