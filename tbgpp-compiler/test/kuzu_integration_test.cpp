#include <iostream>

#include "parser/antlr_parser/kuzu_cypher_parser.h"
#include "cypher_lexer.h"

using namespace antlr4;

int main(int argc, char** argv) {

	std::string query = "MATCH (n) RETURN n;";
	//std::string query = "MATCH (n) RETUddRN n;";

	auto inputStream = ANTLRInputStream(query);
    //auto parserErrorListener = ParserErrorListener();

	// Lexer
    auto cypherLexer = CypherLexer(&inputStream);
    //cypherLexer.removeErrorListeners();
    //cypherLexer.addErrorListener(&parserErrorListener);
    
	auto tokens = CommonTokenStream(&cypherLexer);
    tokens.fill();

	// Parser
	auto kcp = kuzu::parser::KuzuCypherParser(&tokens);

	std::cout << "compiler test end" << std::endl;
	return 0;
}