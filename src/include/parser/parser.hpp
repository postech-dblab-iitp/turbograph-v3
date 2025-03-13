#pragma once

#include "parser/cypher_statement.hpp"
#include "parser/simplified_token.hpp"

namespace duckdb {
//! The parser is responsible for parsing the query and converting it into a set
//! of parsed statements. The parsed statements can then be converted into a
//! plan and executed.
class Parser {
public:
	//! Attempts to parse a query into a series of Cypher statements. Returns
	//! whether or not the parsing was successful. If the parsing was
	//! successful, the parsed statements will be stored in the statements
	//! variable.
	static vector<unique_ptr<CypherStatement>> ParseQuery(string &query);

	//! Returns true if the given text matches a keyword of the parser
	static bool IsKeyword(const string &text);
	//! Returns a list of all keywords in the parser
	static vector<ParserKeyword> KeywordList();
};
} // namespace duckdb
