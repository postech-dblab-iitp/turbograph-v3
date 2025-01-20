#pragma once

#include "parser/parsed_data/create_info.hpp"
#include "common/unordered_set.hpp"
#include "common/enums/extent_type.hpp"

namespace s62 {

struct CreateExtentInfo : public CreateInfo {
	CreateExtentInfo() : CreateInfo(CatalogType::EXTENT_ENTRY, INVALID_SCHEMA) {
	}
	CreateExtentInfo(string schema, string name, ExtentType extent_type_, ExtentID eid_, PartitionID pid, size_t num_tuples_in_extent) 
		: CreateInfo(CatalogType::EXTENT_ENTRY, schema), extent(name), extent_type(extent_type_), eid(eid_), pid(pid), 
		num_tuples_in_extent(num_tuples_in_extent) {
	}

	CreateExtentInfo(string schema, string name, ExtentType extent_type_, ExtentID eid_, PartitionID pid, idx_t ps_oid, size_t num_tuples_in_extent) 
		: CreateInfo(CatalogType::EXTENT_ENTRY, schema), extent(name), extent_type(extent_type_), eid(eid_), pid(pid), ps_oid(ps_oid),
		num_tuples_in_extent(num_tuples_in_extent) {
	}

	PartitionID pid;
	idx_t ps_oid;
	ExtentID eid;
	string extent;
	ExtentType extent_type;
	size_t num_tuples_in_extent;

public:
	unique_ptr<CreateInfo> Copy() const override {
		auto result = make_unique<CreateExtentInfo>(schema, extent, extent_type, eid, pid, ps_oid, num_tuples_in_extent);
		CopyProperties(*result);
		return move(result);
	}
};

} // namespace s62
