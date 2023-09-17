#pragma once
#include "typedef.hpp"

#include "execution/expression_executor.hpp"
#include "execution/physical_operator/cypher_physical_operator.hpp"

#include "common/types/value.hpp"

namespace duckdb {

class PhysicalIdSeek: public CypherPhysicalOperator {

public:
	PhysicalIdSeek(Schema& sch, uint64_t id_col_idx, vector<uint64_t> oids, vector<vector<uint64_t>> projection_mapping,
				   vector<uint32_t> &outer_col_map, vector<uint32_t> &inner_col_map);
	PhysicalIdSeek(Schema& sch, uint64_t id_col_idx, vector<uint64_t> oids, vector<vector<uint64_t>> projection_mapping,
				   vector<uint32_t> &outer_col_map, vector<uint32_t> &inner_col_map, vector<unique_ptr<Expression>> predicates);
	PhysicalIdSeek(Schema& sch, uint64_t id_col_idx, vector<uint64_t> oids, vector<vector<uint64_t>> projection_mapping,
				   vector<uint32_t> &outer_col_map, vector<uint32_t> &inner_col_map, std::vector<duckdb::LogicalType> scan_types,
				   vector<vector<uint64_t>> scan_projection_mapping, int64_t filterKeyIndex, duckdb::Value filterValue);
	~PhysicalIdSeek() {}

public:

	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
	OperatorResultType Execute(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &state) const override;

	std::string ParamsToString() const override;
	std::string ToString() const override;

	// parameters
	uint64_t id_col_idx;
	mutable vector<uint64_t> oids;
	mutable vector<vector<uint64_t>> projection_mapping;

	mutable vector<LogicalType> target_types;	// used to initialize output chunks.

	mutable std::vector<duckdb::LogicalType> scan_types;  		// types scan
	mutable vector<vector<uint64_t>> scan_projection_mapping;	// projection mapping for scan from the storage

	// filter pushdown
	bool do_filter_pushdown;
	bool has_expression = false;
	mutable bool is_tmp_chunk_initialized = false;
	mutable int64_t filter_pushdown_key_idx;	// when negative, no filter pushdown	
	mutable Value filter_pushdown_value;		// do not use when filter_pushdown_key_idx < 0

	vector<uint32_t> outer_col_map;
	vector<uint32_t> inner_col_map;

	unique_ptr<Expression> expression;
	mutable DataChunk tmp_chunk;
	mutable ExpressionExecutor executor;
};

}