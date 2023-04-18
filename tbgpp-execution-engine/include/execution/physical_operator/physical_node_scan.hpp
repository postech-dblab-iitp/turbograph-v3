#pragma once
#include "typedef.hpp"

#include "storage/graph_store.hpp"
#include "main/client_context.hpp"

#include "planner/expression.hpp"
#include "execution/physical_operator/cypher_physical_operator.hpp"

#include <vector>

namespace duckdb {

class PhysicalNodeScan: public CypherPhysicalOperator {

public:
	
	PhysicalNodeScan(CypherSchema& sch, vector<idx_t> oids, vector<vector<uint64_t>> projection_mapping);

	// TODO s62 change filterKeyIndex to vector
	PhysicalNodeScan(CypherSchema& sch, vector<idx_t> oids, vector<vector<uint64_t>> projection_mapping,
		std::vector<duckdb::LogicalType> scan_types, vector<vector<uint64_t>> scan_projection_mapping, 
		int64_t filterKeyIndex, duckdb::Value filterValue);
	PhysicalNodeScan(CypherSchema& sch, vector<idx_t> oids, vector<vector<uint64_t>> projection_mapping, int64_t filterKeyIndex, duckdb::Value filterValue);
	~PhysicalNodeScan();

public:
	
	void GetData(ExecutionContext& context, DataChunk &chunk, LocalSourceState &lstate) const override;
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context) const override;
	bool IsSource() const override { return true; }
	
	std::string ParamsToString() const override;
	std::string ToString() const override;

	// scan parameters
	mutable vector<idx_t> oids;
	mutable vector<vector<uint64_t>> projection_mapping;	// projection mapping for operator output

	mutable std::vector<duckdb::LogicalType> scan_types;  		// types scan
	mutable vector<vector<uint64_t>> scan_projection_mapping;	// projection mapping for scan from the storage

	// filter pushdown
	mutable int64_t filter_pushdown_key_idx;	// when negative, no filter pushdown	
	mutable Value filter_pushdown_value;		// do not use when filter_pushdown_key_idx < 0

	
};	

}