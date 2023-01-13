#include <iostream>

#include "kuzu/parser/antlr_parser/kuzu_cypher_parser.h"
#include "cypher_lexer.h"
#include "parser/transformer.h"


using namespace antlr4;

int main(int argc, char** argv) {

	std::cout << "compiler test start" << std::endl;
	// std::string query = "MATCH (n) RETURN n;";
	std::string query = "EXPLAIN MATCH (n) RETURN n;";
	//std::string query = "MATCH (n) RETURN n;";

	auto inputStream = ANTLRInputStream(query);
    //auto parserErrorListener = ParserErrorListener();

// Lexer
    auto cypherLexer = CypherLexer(&inputStream);
    //cypherLexer.removeErrorListeners();
    //cypherLexer.addErrorListener(&parserErrorListener);
	auto tokens = CommonTokenStream(&cypherLexer);
    tokens.fill();

// Parser
	auto kuzuCypherParser = kuzu::parser::KuzuCypherParser(&tokens);

	// std::cout << (kcp.oC_AnyCypherOption()->oC_Explain() != nullptr) << std::endl;
	// std::cout << (kcp.oC_AnyCypherOption()->oC_Profile() != nullptr) << std::endl;

// Sematic parsing
	// Transformer
	kuzu::parser::Transformer transformer(*kuzuCypherParser.oC_Cypher());
	auto statement = transformer.transform();
	
	// Binder
		// TODO
	std::cout << "compiler test end" << std::endl;
	return 0;
}