//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/export_table_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_data/parse_info.hpp"
#include "common/types/value.hpp"

namespace duckdb {

struct ExportedTableData {
	//! Name of the exported table
	string table_name;

	//! Name of the schema
	string schema_name;

	//! Path to be exported
	string file_path;
};

struct ExportedTableInfo {
	TableCatalogEntry *entry;
	ExportedTableData table_data;
};

struct BoundExportData : public ParseInfo {
	std::vector<ExportedTableInfo> data;
};

} // namespace duckdb
