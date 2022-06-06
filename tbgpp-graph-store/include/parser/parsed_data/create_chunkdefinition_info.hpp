#pragma once

#include "parser/parsed_data/create_info.hpp"
#include "common/unordered_set.hpp"

namespace duckdb {

struct CreateChunkDefinitionInfo : public CreateInfo {
	CreateChunkDefinitionInfo() : CreateInfo(CatalogType::CHUNKDEFINITION_ENTRY, INVALID_SCHEMA) {
	}
	CreateChunkDefinitionInfo(string schema, string name, LogicalType type_) : CreateInfo(CatalogType::CHUNKDEFINITION_ENTRY, schema), chunkdefinition(name), type(type_) {
	}

	string chunkdefinition;
	LogicalType type;

public:
	unique_ptr<CreateInfo> Copy() const override {
		auto result = make_unique<CreateChunkDefinitionInfo>(schema, chunkdefinition);
		CopyProperties(*result);
		return move(result);
	}
};

} // namespace duckdb
