if(POLICY CMP0063)
    cmake_policy(SET CMP0063 NEW)
endif()

add_library(s62_simdcsv STATIC src/io_util.cpp)

target_include_directories(
  s62_simdcsv
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src>)
set_target_properties(s62_simdcsv PROPERTIES EXPORT_NAME s62_simdcsv)

install(TARGETS s62_simdcsv
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
