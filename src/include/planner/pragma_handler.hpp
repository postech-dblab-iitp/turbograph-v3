//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/pragma_handler.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "common/vector.hpp"
#include "parser/statement/pragma_statement.hpp"

// namespace duckdb {
// class ClientContext;
// class ClientContextLock;
// class SQLStatement;
// struct PragmaInfo;

// //! Pragma handler is responsible for converting certain pragma statements into new queries
// class PragmaHandler {
// public:
// 	explicit PragmaHandler(ClientContext &context);

// 	void HandlePragmaStatements(ClientContextLock &lock, vector<unique_ptr<SQLStatement>> &statements);

// private:
// 	ClientContext &context;

// private:
// 	//! Handles a pragma statement, (potentially) returning a new statement to replace the current one
// 	string HandlePragma(SQLStatement *statement);

// 	void HandlePragmaStatementsInternal(vector<unique_ptr<SQLStatement>> &statements);
// };
// } // namespace duckdb
