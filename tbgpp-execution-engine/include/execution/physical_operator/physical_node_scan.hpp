#pragma once
#include "typedef.hpp"

#include "storage/graph_store.hpp"
#include "main/client_context.hpp"

#include "planner/expression.hpp"
#include "execution/physical_operator/cypher_physical_operator.hpp"

#include <vector>

namespace duckdb {

enum class FilterPushdownType: uint8_t {
	FP_EQ,
	FP_RANGE
};

class PhysicalNodeScan: public CypherPhysicalOperator {

public:
	
	PhysicalNodeScan(Schema &sch, vector<idx_t> oids, vector<vector<uint64_t>> projection_mapping);

	// TODO s62 change filterKeyIndex to vector
	PhysicalNodeScan(Schema &sch, vector<idx_t> oids, vector<vector<uint64_t>> projection_mapping,
		std::vector<duckdb::LogicalType> scan_types, vector<vector<uint64_t>> scan_projection_mapping);
	PhysicalNodeScan(Schema &sch, vector<idx_t> oids, vector<vector<uint64_t>> projection_mapping,
		std::vector<duckdb::LogicalType> scan_types, vector<vector<uint64_t>> scan_projection_mapping, 
		int64_t filterKeyIndex, duckdb::Value filterValue);
	PhysicalNodeScan(Schema &sch, vector<idx_t> oids, vector<vector<uint64_t>> projection_mapping,
		std::vector<duckdb::LogicalType> scan_types, vector<vector<uint64_t>> scan_projection_mapping,
		int64_t filterKeyIndex, duckdb::Value l_filterValue,  duckdb::Value r_filterValue, bool l_inclusive, bool r_inclusive);
	PhysicalNodeScan(Schema &sch, vector<idx_t> oids, vector<vector<uint64_t>> projection_mapping, int64_t filterKeyIndex, duckdb::Value filterValue);

	// Schemaless APIs
	PhysicalNodeScan(vector<Schema> &sch, Schema &union_schema, vector<idx_t> oids, vector<vector<uint64_t>> projection_mapping,
		vector<vector<uint64_t>> scan_projection_mapping);

	~PhysicalNodeScan();

public:
	
	void GetData(ExecutionContext& context, DataChunk &chunk, LocalSourceState &lstate) const override;
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context) const override;
	bool IsSource() const override { return true; }
	
	string ParamsToString() const override;
	string ToString() const override;

	// scan parameters
	mutable vector<idx_t> oids;
	mutable vector<vector<uint64_t>> projection_mapping;	// projection mapping for operator output

	mutable vector<vector<LogicalType>> scan_types;  		// types scan
	mutable vector<vector<uint64_t>> scan_projection_mapping;	// projection mapping for scan from the storage

	// filter pushdown
	mutable FilterPushdownType filter_pushdown_type;
	mutable int64_t filter_pushdown_key_idx;	// when negative, no filter pushdown	
	mutable Value filter_pushdown_value;		// do not use when filter_pushdown_key_idx < 0
	mutable RangeFilterValue range_filter_pushdown_value;

	mutable int64_t current_schema_idx;
	mutable int64_t num_schemas;
};	

}