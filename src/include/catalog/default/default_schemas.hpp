//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/default/default_schemas.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/default/default_generator.hpp"

namespace duckdb {

class DefaultSchemaGenerator : public DefaultGenerator {
public:
	explicit DefaultSchemaGenerator(Catalog &catalog);

public:
	unique_ptr<CatalogEntry> CreateDefaultEntry(ClientContext &context, const string &entry_name) override;
	vector<string> GetDefaultEntries() override;
};

} // namespace duckdb
