#include "parser/parser.hpp"
#include "common/string_util.hpp"

namespace duckdb {

using namespace antlr4;

void Parser::ParseQuery(string &query) {
    StringUtil::LTrim(query);
    if (query.empty()) {
        return;
    }
}

} // namespace duckdb
