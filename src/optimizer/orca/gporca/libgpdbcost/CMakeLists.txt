file(GLOB_RECURSE srcs ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)

add_library(gpdbcost SHARED ${srcs})

target_link_libraries(gpdbcost  
                      ${CMAKE_DL_LIBS}
                      gpos
                      ${XERCES_LIBRARIES})