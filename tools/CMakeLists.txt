file(GLOB SRC_FILES "*.c*")

if(NOT ${BUILD_ANALYTICS})
    list(FILTER SRC_FILES EXCLUDE REGEX ".*pr_int.cpp")
endif()

foreach(SRC_FILE ${SRC_FILES})
    get_filename_component(EXEC_NAME ${SRC_FILE} NAME_WE)
    add_executable(${EXEC_NAME} ${SRC_FILE})
    target_link_libraries(${EXEC_NAME} s62gdb)
endforeach()