add_compile_options(-DUSE_CMAKE)
# Turn on platform-specific defines.
set(GPOS_${CMAKE_SYSTEM_NAME} 1)
set(GPOS_${CMAKE_SYSTEM_PROCESSOR} 1)

# # Library dependencies for optimizer.
set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

find_package(Xerces)

IF (${XERCES_FOUND})
   MESSAGE(STATUS "found gp-xerces")
ELSE ()
  MESSAGE(STATUS "could not locate gp-xerces. Installing from third_party/ ...")
  # install gp-xerces
  execute_process(COMMAND mkdir -p build
                  WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/third_party/gp-xerces")
  execute_process(COMMAND ../configure --prefix=/usr/local
                  WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/third_party/gp-xerces/build")
  execute_process(COMMAND make
                  WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/third_party/gp-xerces/build")
  # TODO this may fail due to permission issue.
  execute_process(COMMAND make install
                  WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/third_party/gp-xerces/build")
ENDIF ()

find_package(Xerces REQUIRED)
include_directories(${XERCES_INCLUDE_DIRS})

# Sub-libraries.
add_subdirectory(libgpos)
add_subdirectory(libnaucrates)
add_subdirectory(libgpdbcost)
add_subdirectory(libgpopt)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES}
    PARENT_SCOPE)