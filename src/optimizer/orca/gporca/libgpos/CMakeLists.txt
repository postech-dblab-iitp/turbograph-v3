file(GLOB_RECURSE srcs ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)

add_library(gpos SHARED ${srcs})

target_link_libraries(gpos ${CMAKE_DL_LIBS} -ltcmalloc)