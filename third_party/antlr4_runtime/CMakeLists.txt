include_directories(
  "${CMAKE_CURRENT_SOURCE_DIR}/src"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/atn"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/dfa"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/internal"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/misc"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/support"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/tree"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/tree/pattern"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/tree/xpath"
)

file(GLOB libantlrcpp_SRC
  "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/atn/*.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/dfa/*.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/internal/*.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/misc/*.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/support/*.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/tree/*.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/tree/pattern/*.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/tree/xpath/*.cpp"
)
message("${CMAKE_CURRENT_SOURCE_DIR}")
add_library(antlr4_runtime STATIC)
target_sources(antlr4_runtime PUBLIC "${libantlrcpp_SRC}")

# Make sure to link against threads (pthreads) library in order to be able to
# make use of std::call_once in the code without producing runtime errors
# (see also https://github.com/antlr/antlr4/issues/3708 and/or https://stackoverflow.com/q/51584960).
find_package(Threads REQUIRED)
target_link_libraries(antlr4_runtime Threads::Threads)

set(extra_share_compile_flags "")
set(extra_static_compile_flags "")
set(static_lib_suffix "")


set_target_properties(antlr4_runtime
                      PROPERTIES VERSION   ${ANTLR_VERSION}
                                 SOVERSION ${ANTLR_VERSION}
                                 OUTPUT_NAME "antlr4-runtime${static_lib_suffix}"
                                 COMPILE_PDB_NAME "antlr4-runtime${static_lib_suffix}"
                                 COMPILE_FLAGS "${disabled_compile_warnings} ${extra_static_compile_flags}")

install(TARGETS antlr4_runtime
        EXPORT antlr4-targets
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})

install(DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/src/"
        DESTINATION "include/antlr4-runtime"
        COMPONENT dev
        FILES_MATCHING PATTERN "*.h"
        )