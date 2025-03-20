// #include "parser/statement/vacuum_statement.hpp"

// namespace duckdb {

// VacuumStatement::VacuumStatement(const VacuumOptions &options)
//     : CypherStatement(StatementType::VACUUM_STATEMENT), info(make_unique<VacuumInfo>(options)) {
// }

// VacuumStatement::VacuumStatement(const VacuumStatement &other) : CypherStatement(other), info(other.info->Copy()) {
// }

// unique_ptr<CypherStatement> VacuumStatement::Copy() const {
// 	return unique_ptr<VacuumStatement>(new VacuumStatement(*this));
// }

// } // namespace duckdb
