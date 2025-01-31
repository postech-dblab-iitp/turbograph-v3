cmake_minimum_required(VERSION 3.16)

####################################################
# PROJECT METADATA 
####################################################
project(client)

####################################################
# DECLARE TARGETS
####################################################
add_library(tbgpp-main STATIC)

####################################################
# PER-TARGET DEFINITION - tbgpp-main
####################################################
file (GLOB_RECURSE SYSTEM_SOURCES "${PROJECT_SOURCE_DIR}/src/*.c*")
target_sources(tbgpp-main PUBLIC ${SYSTEM_SOURCES})
target_link_libraries(tbgpp-main "-lrt -lpthread -ltbb -fopenmp -laio -lnuma -lhwloc -ltcmalloc")
target_link_libraries(tbgpp-main common-lib)
target_link_libraries(tbgpp-main storage-lib)
target_link_libraries(tbgpp-main execution-lib)
target_link_libraries(tbgpp-main compiler)

target_compile_options(tbgpp-main PUBLIC ${CXX_FLAGS})
target_compile_definitions(tbgpp-main PUBLIC ${CXX_DEFINITION})

####################################################
# TESTS
####################################################
file(GLOB CLIENT_TEST_SRC
  "${PROJECT_SOURCE_DIR}/test/TurboGraph-S62.cpp"
)

foreach(test_file ${CLIENT_TEST_SRC})
  get_filename_component(prog_name ${test_file} NAME_WE)
  add_executable(${prog_name} ${test_file} )
  target_sources( ${prog_name} PUBLIC ${SOURCES} )
  target_sources( ${prog_name} PUBLIC ${TEST_SOURCES}  ) 
  target_compile_options(${prog_name} PRIVATE ${CXX_FLAGS})
  target_compile_definitions(${prog_name} PRIVATE ${CXX_DEFINITION})
  target_link_libraries(${prog_name} PRIVATE
    tbgpp-main
    common-lib
    storage-lib
    execution-lib
    nlohmann_json::nlohmann_json "-pthread -ldl -lboost_system -lboost_timer -lboost_date_time -lboost_filesystem -ltbb"
  )
endforeach(test_file ${CLIENT_TEST_SRC})
