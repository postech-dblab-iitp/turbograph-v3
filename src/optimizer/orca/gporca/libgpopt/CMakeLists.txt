file(GLOB_RECURSE srcs ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)

add_library(gpopt SHARED ${srcs})

target_link_libraries(gpopt
                      gpdbcost
                      naucrates
                      ${CMAKE_DL_LIBS}
                      gpos
                      ${XERCES_LIBRARIES})
