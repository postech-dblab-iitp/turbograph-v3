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
    s62_simdjson
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
    openmp
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
)

set(S62GDB_LINK_LIBS
    ${S62GDB_SYSTEM_LIBS}
    ${S62GDB_THIRD_PARTY_LIBS}
    ${S62GDB_INSTALLED_LIBS}
)

add_library(s62gdb SHARED ${ALL_OBJECT_FILES})
add_library(s62gdb_static STATIC ${ALL_OBJECT_FILES})


target_include_directories(
    s62gdb PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
                  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)

target_include_directories(
    s62gdb_static PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
                $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)

install(
    DIRECTORY "${PROJECT_SOURCE_DIR}/src/include"
    DESTINATION "${INSTALL_INCLUDE_DIR}"
    FILES_MATCHING
    PATTERN "*.hpp"
    PATTERN "*.ipp")

install(
    TARGETS s62gdb s62gdb_static
    EXPORT "${S62_EXPORT_SET}"
    LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
    ARCHIVE DESTINATION "${INSTALL_LIB_DIR}"
    RUNTIME DESTINATION "${INSTALL_BIN_DIR}")