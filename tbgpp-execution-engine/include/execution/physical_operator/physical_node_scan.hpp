#pragma once
#include "typedef.hpp"

#include "storage/graph_store.hpp"
#include "main/client_context.hpp"
#include "planner/expression.hpp"
#include "execution/physical_operator/cypher_physical_operator.hpp"
#include "execution/expression_executor.hpp"

#include <vector>

namespace duckdb {

class PhysicalNodeScan: public CypherPhysicalOperator {

public:
	/**
	 * Non-schemaless APIs
	*/
	PhysicalNodeScan(Schema &sch, vector<idx_t> oids, vector<vector<uint64_t>> projection_mapping,
		std::vector<duckdb::LogicalType> scan_types, vector<vector<uint64_t>> scan_projection_mapping);
	
	// eq filter pushdown
	PhysicalNodeScan(Schema &sch, vector<idx_t> oids, vector<vector<uint64_t>> projection_mapping,
		std::vector<duckdb::LogicalType> scan_types, vector<vector<uint64_t>> scan_projection_mapping, 
		int64_t filterKeyIndex, duckdb::Value filterValue);

	// range filter pushdown
	PhysicalNodeScan(Schema &sch, vector<idx_t> oids, vector<vector<uint64_t>> projection_mapping,
		std::vector<duckdb::LogicalType> scan_types, vector<vector<uint64_t>> scan_projection_mapping,
		int64_t filterKeyIndex, duckdb::Value l_filterValue,  duckdb::Value r_filterValue, bool l_inclusive, bool r_inclusive);

	// complex filter pushdown
	PhysicalNodeScan(Schema &sch, vector<idx_t> oids, vector<vector<uint64_t>> projection_mapping,
		std::vector<duckdb::LogicalType> scan_types, vector<vector<uint64_t>> scan_projection_mapping,
		vector<unique_ptr<Expression>> predicates);

	/**
	 * Schemaless APIs
	*/
	PhysicalNodeScan(vector<Schema> &sch, Schema &union_schema, vector<idx_t> oids, vector<vector<uint64_t>> projection_mapping,
		vector<vector<uint64_t>> scan_projection_mapping);

	// eq filter pushdown
	PhysicalNodeScan(vector<Schema> &sch, Schema &union_schema, vector<idx_t> oids, vector<vector<uint64_t>> projection_mapping,
		vector<vector<uint64_t>> scan_projection_mapping, vector<int64_t>& filterKeyIndexes, vector<duckdb::Value>& filterValues);

	// range filter pushdown
	PhysicalNodeScan(vector<Schema> &sch, Schema &union_schema, vector<idx_t> oids, vector<vector<uint64_t>> projection_mapping,
		vector<vector<uint64_t>> scan_projection_mapping, vector<int64_t>& filterKeyIndexes, vector<RangeFilterValue>& rangeFilterValues);

	// complex filter pushdown
	PhysicalNodeScan(vector<Schema> &sch, Schema &union_schema, vector<idx_t> oids, vector<vector<uint64_t>> projection_mapping,
		vector<vector<uint64_t>> scan_projection_mapping, vector<unique_ptr<Expression>> predicates);

	~PhysicalNodeScan();

public:
	/**
	 * Source APIs
	*/
	void GetData(ExecutionContext& context, DataChunk &chunk, LocalSourceState &lstate) const override;
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context) const override;
	bool IsSource() const override { return true; }
	bool IsSourceDataRemaining(LocalSourceState &lstate) const override;
	
	/**
	 * ETC APIs
	*/
	string ParamsToString() const override;
	string ToString() const override;

	// scan parameters
	mutable vector<idx_t> oids;
	mutable vector<vector<uint64_t>> projection_mapping;	// projection mapping for operator output
	mutable vector<vector<LogicalType>> scan_types;  		// types scan
	mutable vector<vector<uint64_t>> scan_projection_mapping;	// projection mapping for scan from the storage

	// filter pushdown
	bool is_filter_pushdowned = false;
	mutable FilterPushdownType filter_pushdown_type;
	mutable FilterKeyIdxs filter_pushdown_key_idxs;	// when negative, no filter pushdown
	mutable EQFilterValues eq_filter_pushdown_values;		// do not use when filter_pushdown_key_idx < 0
	mutable RangeFilterValues range_filter_pushdown_values;
	mutable unique_ptr<Expression> filter_expression;
	mutable ExpressionExecutor executor;

	// filtered chunk buffers
	mutable FilteredChunkBuffer filtered_chunk_buffer;

	mutable int64_t current_schema_idx;
	mutable int64_t num_schemas;
};	

}