add_subdirectory(gporca)

# add libraries
add_library(orca STATIC)
file (GLOB_RECURSE SOURCES "gpopt/*.c*" "nodes/*.c*" "utils/*.c*")
target_sources(orca PUBLIC ${SOURCES})

target_link_libraries(orca PUBLIC gpos naucrates gpdbcost gpopt tbgpp-main common-lib storage-lib)
