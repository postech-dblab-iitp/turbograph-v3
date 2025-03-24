#include <unordered_set>

#include "parser/parser.hpp"
#include "common/string_util.hpp"
#include "common/exception.hpp"
#include "common/logger.hpp"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include "cypher_lexer.h"
#pragma GCC diagnostic pop

#include "parser/antlr_parser/s62_cypher_parser.h"
#include "parser/antlr_parser/parser_error_listener.h"
#include "parser/antlr_parser/parser_error_strategy.h"
#include "parser/transformer.hpp"

namespace duckdb {

using namespace antlr4;

vector<shared_ptr<CypherStatement>> Parser::ParseQuery(string query) {
    StringUtil::LTrim(query);
    if (query.empty()) {
        throw ParserException("Cannot parse empty query.");
    }

    auto inputStream = ANTLRInputStream(query);
    auto parserErrorListener = ParserErrorListener();

    spdlog::debug("[ParseQuery] ANTLRInputStream created");

    CypherLexer cypherLexer(&inputStream);
    cypherLexer.removeErrorListeners();
    cypherLexer.addErrorListener(&parserErrorListener);
    CommonTokenStream tokens(&cypherLexer);
    tokens.fill();

    spdlog::debug("[ParseQuery] tokens generated");

    S62CypherParser s62CypherParser(&tokens);
    s62CypherParser.removeErrorListeners();
    s62CypherParser.addErrorListener(&parserErrorListener);
    s62CypherParser.setErrorHandler(std::make_shared<ParserErrorStrategy>());

    spdlog::debug("[ParseQuery] parsing done");

    Transformer transformer(*s62CypherParser.ku_Statements());
    auto statements = transformer.transform();
    if (statements.size() > 1) {
        throw ParserException("Cannot parse multiple statements at once.");
    }
    for (auto& statement : statements) {
        spdlog::debug("[ParseQuery] transformed statement");
        spdlog::debug("\t" + statement->ToString());
    }
    return statements;
}

static const std::unordered_set<string> _keywords = {
    "ACYCLIC", "ANY", "ADD", "ALL", "ALTER", "AND", "AS", "ASC", 
    "ASCENDING", "ATTACH", "BEGIN", "BY", "CALL", "CASE", "CAST", 
    "CHECKPOINT", "COLUMN", "COMMENT", "COMMIT", "COMMIT_SKIP_CHECKPOINT", 
    "CONTAINS", "COPY", "COUNT", "CREATE", "CYCLE", "DATABASE", "DBTYPE", 
    "DEFAULT", "DELETE", "DESC", "DESCENDING", "DETACH", "DISTINCT", 
    "DROP", "ELSE", "END", "ENDS", "EXISTS", "EXPLAIN", "EXPORT", "EXTENSION", 
    "FALSE", "FROM", "GLOB", "GRAPH", "GROUP", "HEADERS", "HINT", "IMPORT", "IF", 
    "IN", "INCREMENT", "INSTALL", "IS", "JOIN", "KEY", "LIMIT", "LOAD", 
    "LOGICAL", "MACRO", "MATCH", "MAXVALUE", "MERGE", "MINVALUE", "MULTI_JOIN", 
    "NO", "NODE", "NOT", "NONE", "NULL", "ON", "ONLY", "OPTIONAL", "OR", "ORDER",
     "PRIMARY", "PROFILE", "PROJECT", "READ", "REL", "RENAME", "RETURN", 
     "ROLLBACK", "ROLLBACK_SKIP_CHECKPOINT", "SEQUENCE", "SET", "SHORTEST", 
     "START", "STARTS", "TABLE", "THEN", "TO", "TRAIL", "TRANSACTION", "TRUE", 
     "TYPE", "UNION", "UNWIND", "USE", "WHEN", "WHERE", "WITH", "WRITE", "WSHORTEST", 
     "XOR", "SINGLE"
};


bool Parser::IsKeyword(const string &text) {
    return _keywords.find(text) != _keywords.end();
}

} // namespace duckdb
