set(ANTLR4_RUNTIME_SRC
        antlr4/CypherBaseListener.cpp
        antlr4/CypherLexer.cpp
        antlr4/CypherParser.cpp
        antlr4/CypherListener.cpp
)

add_library(antlr4_cypher STATIC ${ANTLR4_RUNTIME_SRC})
target_include_directories(antlr4_cypher PRIVATE antlr4)
target_include_directories(antlr4_cypher
        PRIVATE ../antlr4_runtime/src)

target_link_libraries(antlr4_cypher PRIVATE antlr4_runtime)