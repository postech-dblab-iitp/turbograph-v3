#include "parser/parser.hpp"
// #include "common/string_util.hpp"
// #include "common/exception.hpp"

// #pragma GCC diagnostic push
// #pragma GCC diagnostic ignored "-Wunused-parameter"
// #include "cypher_lexer.h"
// #pragma GCC diagnostic pop

// #include "parser/antlr_parser/s62_cypher_parser.h"
// #include "parser/antlr_parser/parser_error_listener.h"
// #include "parser/antlr_parser/parser_error_strategy.h"

namespace duckdb {

// using namespace antlr4;

vector<unique_ptr<CypherStatement>> Parser::ParseQuery(string &query) {
    // StringUtil::LTrim(query);
    // if (query.empty()) {
    //     throw ParserException("Cannot parse empty query.");
    // }

    // auto inputStream = ANTLRInputStream(query);
    // auto parserErrorListener = ParserErrorListener();

    // auto cypherLexer = CypherLexer(&inputStream);
    // cypherLexer.removeErrorListeners();
    // cypherLexer.addErrorListener(&parserErrorListener);
    // auto tokens = CommonTokenStream(&cypherLexer);
    // tokens.fill();

    // auto s62CypherParser = S62CypherParser(&tokens);
    // s62CypherParser.removeErrorListeners();
    // s62CypherParser.addErrorListener(&parserErrorListener);
    // s62CypherParser.setErrorHandler(std::make_shared<ParserErrorStrategy>());

    // Transformer transformer(*s62CypherParser.ku_Statements());
    // return transformer.transform();
}

bool Parser::IsKeyword(const string &text) {
	return true;
	//return PostgresParser::IsKeyword(text);
}

vector<ParserKeyword> Parser::KeywordList() {
	//auto keywords = PostgresParser::KeywordList();
	vector<ParserKeyword> result;
	return result;
}

} // namespace duckdb
