file(GLOB_RECURSE srcs ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)

add_library(naucrates SHARED ${srcs})

target_link_libraries(naucrates
                      ${CMAKE_DL_LIBS}
                      gpos
                      ${XERCES_LIBRARIES})