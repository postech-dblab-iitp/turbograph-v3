#pragma once

#include "common/assert.hpp"
#include "common/value_operations/value_operations.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "execution/expression_executor.hpp"
// #include "storage/data_table.hpp"
#include "common/sort/sort.hpp"
#include "common/types/row_layout.hpp"

#include "common/types/data_chunk.hpp"
#include "common/sort/sorted_block.hpp"
#include "common/sort/sort.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Heaps
//===--------------------------------------------------------------------===//
class TopNHeap;

struct TopNScanState {
	unique_ptr<PayloadScanner> scanner;
	idx_t pos;
	bool exclude_offset;
	bool is_finished = false;
};

class TopNSortState {
public:
	explicit TopNSortState(TopNHeap &heap);

	TopNHeap &heap;
	unique_ptr<LocalSortState> local_state;
	unique_ptr<GlobalSortState> global_state;
	idx_t count;
	bool is_sorted;

public:
	void Initialize();
	void Append(DataChunk &sort_chunk, DataChunk &payload);

	void Sink(DataChunk &input);
	void Finalize();

	void Move(TopNSortState &other);

	void InitializeScan(TopNScanState &state, bool exclude_offset);
	void Scan(TopNScanState &state, DataChunk &chunk);
	
	bool IsEnd(TopNScanState &state);
};

class TopNHeap {
public:
	TopNHeap(ClientContext &context, const vector<LogicalType> &payload_types, const vector<BoundOrderByNode> &orders,
	         idx_t limit, idx_t offset);

	ClientContext &context;
	const vector<LogicalType> &payload_types;
	const vector<BoundOrderByNode> &orders;
	idx_t limit;
	idx_t offset;
	TopNSortState sort_state;
	ExpressionExecutor executor;
	DataChunk sort_chunk;
	DataChunk compare_chunk;
	DataChunk payload_chunk;
	//! A set of boundary values that determine either the minimum or the maximum value we have to consider for our
	//! top-n
	DataChunk boundary_values;
	//! Whether or not the boundary_values has been set. The boundary_values are only set after a reduce step
	bool has_boundary_values;

	SelectionVector final_sel;
	SelectionVector true_sel;
	SelectionVector false_sel;
	SelectionVector new_remaining_sel;

public:
	void Sink(DataChunk &input);
	void Combine(TopNHeap &other);
	void Reduce();
	void Finalize();

	void ExtractBoundaryValues(DataChunk &current_chunk, DataChunk &prev_chunk);

	void InitializeScan(TopNScanState &state, bool exclude_offset);
	void Scan(TopNScanState &state, DataChunk &chunk);

	bool CheckBoundaryValues(DataChunk &sort_chunk, DataChunk &payload);

	bool IsEnd(TopNScanState &state);
};


} // namespace duckdb