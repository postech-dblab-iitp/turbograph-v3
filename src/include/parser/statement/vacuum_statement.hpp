// //===----------------------------------------------------------------------===//
// //                         DuckDB
// //
// // duckdb/parser/statement/vacuum_statement.hpp
// //
// //
// //===----------------------------------------------------------------------===//

// #pragma once

// #include "parser/parsed_expression.hpp"
// #include "parser/cypher_statement.hpp"
// #include "parser/parsed_data/vacuum_info.hpp"

// namespace duckdb {

// class VacuumStatement : public CypherStatement {
// public:
// 	explicit VacuumStatement(const VacuumOptions &options);

// 	unique_ptr<VacuumInfo> info;

// protected:
// 	VacuumStatement(const VacuumStatement &other);

// public:
// 	unique_ptr<CypherStatement> Copy() const override;
// };

// } // namespace duckdb
