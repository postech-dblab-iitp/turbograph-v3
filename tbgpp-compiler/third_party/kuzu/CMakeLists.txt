add_library(kuzu STATIC)

target_include_directories(kuzu PUBLIC
	"include"
	"${CMAKE_CURRENT_SOURCE_DIR}/include/kuzu"
)
target_include_directories(kuzu PRIVATE ../antlr4_runtime/src)
target_include_directories(kuzu PRIVATE ../antlr4_cypher/include)

file (GLOB_RECURSE SOURCES "src/kuzu/*.c*")
target_sources(kuzu PUBLIC ${SOURCES})

target_link_libraries(kuzu PRIVATE antlr4_runtime antlr4_cypher)