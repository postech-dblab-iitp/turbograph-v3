#pragma once

#include "parser/parsed_data/create_info.hpp"
#include "common/unordered_set.hpp"

namespace duckdb {

struct CreatePropertySchemaInfo : public CreateInfo {
	CreatePropertySchemaInfo() : CreateInfo(CatalogType::PROPERTY_SCHEMA_ENTRY, INVALID_SCHEMA) {
	}
	CreatePropertySchemaInfo(string schema, string name, PartitionID pid) : CreateInfo(CatalogType::PROPERTY_SCHEMA_ENTRY, schema),
		propertyschema(name), pid(pid) {
	}

	string propertyschema;
	PartitionID pid;

public:
	unique_ptr<CreateInfo> Copy() const override {
		auto result = make_unique<CreatePropertySchemaInfo>(schema, propertyschema, pid);
		CopyProperties(*result);
		return move(result);
	}
};

} // namespace duckdb
