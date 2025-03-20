#include "parser/statement/extension_statement.hpp"

namespace duckdb {

// ExtensionStatement::ExtensionStatement(ParserExtension extension_p, unique_ptr<ParserExtensionParseData> parse_data_p)
//     : CypherStatement(StatementType::EXTENSION_STATEMENT), extension(move(extension_p)), parse_data(move(parse_data_p)) {
// }

// unique_ptr<CypherStatement> ExtensionStatement::Copy() const {
// 	return make_unique<ExtensionStatement>(extension, parse_data->Copy());
// }

} // namespace duckdb
