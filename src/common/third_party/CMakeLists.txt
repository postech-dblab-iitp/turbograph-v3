# don't export any symbols from the third party stuff
set(CMAKE_C_VISIBILITY_PRESET hidden)

add_subdirectory(fastpforlib)
add_subdirectory(simdcsv)