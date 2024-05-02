cmake_minimum_required(VERSION 3.16)

####################################################
# PROJECT METADATA 
####################################################
project(tbgpp-client)

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
add_library(tbgpp-api-server SHARED)

####################################################
# GLOBAL DEFINITION
####################################################

####################################################
# PER-TARGET DEFINITION - tbgpp-server
####################################################
target_include_directories(tbgpp-api-server PUBLIC
	"${PROJECT_SOURCE_DIR}/include"
	"${PROJECT_SOURCE_DIR}/../tbgpp-common/include"
)
file (GLOB_RECURSE SYSTEM_SOURCES "${PROJECT_SOURCE_DIR}/src/*.c*")
target_sources(tbgpp-api-server PUBLIC ${SYSTEM_SOURCES})
target_link_libraries(tbgpp-api-server tbgpp-main)
target_link_libraries(tbgpp-api-server tbgpp-common-lib)
target_link_libraries(tbgpp-api-server tbgpp-graph-store-lib)
target_link_libraries(tbgpp-api-server tbgpp-execution-engine-lib)
target_link_libraries(tbgpp-api-server nlohmann_json::nlohmann_json)
target_link_libraries(tbgpp-api-server "-lrt -pthread -ldl -lboost_system -lboost_timer -lboost_date_time -lboost_filesystem -ltbb -fopenmp -laio -lnuma -lhwloc")

target_compile_options(tbgpp-api-server PUBLIC ${CXX_FLAGS})
target_compile_definitions(tbgpp-api-server PUBLIC ${CXX_DEFINITION})

####################################################
# TESTS
####################################################
file(GLOB CLIENT_TEST_SRC
  "${PROJECT_SOURCE_DIR}/test/s62-c-api-test.c"
  "${PROJECT_SOURCE_DIR}/test/s62-c-api-test-1.cpp"
)

foreach(test_file ${CLIENT_TEST_SRC})
  get_filename_component(prog_name ${test_file} NAME_WE)
  add_executable(${prog_name} ${test_file} )
  target_sources( ${prog_name} PUBLIC ${SOURCES} )
  target_sources( ${prog_name} PUBLIC ${TEST_SOURCES}  ) 
  target_compile_options(${prog_name} PRIVATE ${C_FLAGS})
  target_compile_definitions(${prog_name} PRIVATE ${C_DEFINITION})
  target_include_directories( ${prog_name} PUBLIC
	"${PROJECT_SOURCE_DIR}/include"
  )
  target_link_libraries(${prog_name} PRIVATE
	  tbgpp-api-server
  )
endforeach(test_file ${CLIENT_TEST_SRC})