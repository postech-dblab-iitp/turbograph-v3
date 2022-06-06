#pragma once

#include "parser/parsed_data/create_info.hpp"
#include "common/unordered_set.hpp"

namespace duckdb {

struct CreateExtentInfo : public CreateInfo {
	CreateExtentInfo() : CreateInfo(CatalogType::EXTENT_ENTRY, INVALID_SCHEMA) {
	}
	CreateExtentInfo(string schema, string name, ExtentType extent_type_, ExtentID eid_) : CreateInfo(CatalogType::EXTENT_ENTRY, schema), extent(name), 
		extent_type(extent_type_), eid(eid_) {
	}

	ExtentID eid;
	string extent;
	ExtentType extent_type;

public:
	unique_ptr<CreateInfo> Copy() const override {
		auto result = make_unique<CreateExtentInfo>(schema, extent);
		CopyProperties(*result);
		return move(result);
	}
};

} // namespace duckdb
