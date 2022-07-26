if(POLICY CMP0063)
    cmake_policy(SET CMP0063 NEW)
endif()

add_library(turbograph_simdcsv STATIC src/io_util.cpp)

target_include_directories(
        turbograph_simdcsv
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src>)
set_target_properties(turbograph_simdcsv PROPERTIES EXPORT_NAME turbograph_simdcsv)

install(TARGETS turbograph_simdcsv
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

      #disable_target_warnings(turbograph_simdcsv)
