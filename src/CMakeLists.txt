add_subdirectory(catalog)
add_subdirectory(common)
add_subdirectory(execution)
add_subdirectory(function)
add_subdirectory(main)
add_subdirectory(optimizer)
add_subdirectory(parser)
add_subdirectory(planner)
add_subdirectory(storage)

set(S62GDB_SYSTEM_LIBS ${CMAKE_DL_LIBS})

set(S62GDB_THIRD_PARTY_LIBS
    antlr4_cypher
    antlr4_runtime
    cuckoofilter
    duckdb_fmt
    duckdb_re2
    duckdb_fastpforlib
    nlohmann_json::nlohmann_json
    s62_simdcsv
    duckdb_utf8proc
    velox_dwio_common
    duckdb_yyjson
)

set(S62GDB_INSTALLED_LIBS
    pthread
    numa
    rt
    hwloc
    tbb
    aio
    tcmalloc
    boost_system
    boost_timer
    boost_date_time
    boost_filesystem
    ${Python3_LIBRARIES}
    crypto
    readline
    ssl
    simdjson
    ${XERCES_LIBRARIES}
)

set(S62GDB_LINK_LIBS
    ${S62GDB_SYSTEM_LIBS}
    ${S62GDB_THIRD_PARTY_LIBS}
    ${S62GDB_INSTALLED_LIBS}
)

add_library(s62gdb SHARED ${ALL_OBJECT_FILES})

target_link_libraries(s62gdb ${S62GDB_LINK_LIBS})

target_include_directories(
    s62gdb PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
                  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)

if(${BUILD_STATIC})
    add_library(s62gdb_static STATIC ${ALL_OBJECT_FILES})
    target_link_libraries(s62gdb_static ${S62GDB_LINK_LIBS})
    target_link_options(s62gdb_static PRIVATE -static)
    target_include_directories(
    s62gdb_static PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
                $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
endif()