add_library(kuzu STATIC)

target_include_directories(kuzu PUBLIC
	"include"
	"${CMAKE_CURRENT_SOURCE_DIR}/include/kuzu"
)
target_include_directories(kuzu PRIVATE ../third_party/antlr4_runtime/src)
target_include_directories(kuzu PRIVATE ../third_party/antlr4_cypher/antlr4)
target_include_directories(kuzu PRIVATE ../../tbgpp-client/include/main)


file (GLOB_RECURSE SOURCES "src/kuzu/*.c*")
target_sources(kuzu PUBLIC ${SOURCES})

target_link_libraries(kuzu PRIVATE antlr4_runtime antlr4_cypher)
target_link_libraries(kuzu PRIVATE tbgpp-main)