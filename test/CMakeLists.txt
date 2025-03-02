include_directories(../third_party/catch)

add_executable(unittest unittest.cpp)

target_link_libraries(unittest s62gdb)