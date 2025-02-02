add_subdirectory(gporca)

# add libraries
file (GLOB_RECURSE ORCA_SOURCES "gpopt/*.c*" "nodes/*.c*" "utils/*.c*")

add_library_unity(
  s62gdb_orca
  OBJECT
  ${ORCA_SOURCES}
)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:s62gdb_orca>
    PARENT_SCOPE)