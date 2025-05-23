//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/row_operations/row_operations.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/enums/expression_type.hpp"
#include "common/types.hpp"

#include "icecream.hpp"

namespace duckdb {

struct AggregateObject;
class DataChunk;
class RowLayout;
class RowDataCollection;
struct SelectionVector;
class StringHeap;
class Vector;
struct VectorData;

// RowOperations contains a set of operations that operate on data using a RowLayout
struct RowOperations {
	//===--------------------------------------------------------------------===//
	// Aggregation Operators
	//===--------------------------------------------------------------------===//
	//! initialize - unaligned addresses
	static void InitializeStates(RowLayout &layout, Vector &addresses, const SelectionVector &sel, idx_t count);
	//! destructor - unaligned addresses, updated
	static void DestroyStates(RowLayout &layout, Vector &addresses, idx_t count);
	//! update - aligned addresses
	static void UpdateStates(AggregateObject &aggr, Vector &addresses, DataChunk &payload, idx_t arg_idx, idx_t count);
	//! filtered update - aligned addresses
	static void UpdateFilteredStates(AggregateObject &aggr, Vector &addresses, DataChunk &payload, idx_t arg_idx);
	//! combine - unaligned addresses, updated
	static void CombineStates(RowLayout &layout, Vector &sources, Vector &targets, idx_t count);
	//! finalize - unaligned addresses, updated
	static void FinalizeStates(RowLayout &layout, Vector &addresses, DataChunk &result, idx_t aggr_idx);

	//===--------------------------------------------------------------------===//
	// Read/Write Operators
	//===--------------------------------------------------------------------===//
	//! Scatter group data to the rows. Initialises the ValidityMask.
	static void Scatter(DataChunk &columns, VectorData col_data[], const RowLayout &layout, Vector &rows,
	                    RowDataCollection &string_heap, const SelectionVector &sel, idx_t count);
	//! Gather a single column.
	static void Gather(Vector &rows, const SelectionVector &row_sel, Vector &col, const SelectionVector &col_sel,
	                   const idx_t count, const idx_t col_offset, const idx_t col_no, const idx_t build_size = 0);
	//! Full Scan an entire columns
	static void FullScanColumn(const RowLayout &layout, Vector &rows, Vector &col, idx_t count, idx_t col_idx);

	//===--------------------------------------------------------------------===//
	// Comparison Operators
	//===--------------------------------------------------------------------===//
	//! Compare a block of key data against the row values to produce an updated selection that matches
	//! and a second (optional) selection of non-matching values.
	//! Returns the number of matches remaining in the selection.
	using Predicates = vector<ExpressionType>;

	static idx_t Match(DataChunk &columns, VectorData col_data[], const RowLayout &layout, Vector &rows,
	                   const Predicates &predicates, SelectionVector &sel, idx_t count, SelectionVector *no_match,
	                   idx_t &no_match_count);

	//===--------------------------------------------------------------------===//
	// Heap Operators
	//===--------------------------------------------------------------------===//
	//! Compute the entry sizes of a vector with variable size type (used before building heap buffer space).
	static void ComputeEntrySizes(Vector &v, idx_t entry_sizes[], idx_t vcount, idx_t ser_count,
	                              const SelectionVector &sel, idx_t offset = 0);
	//! Compute the entry sizes of vector data with variable size type (used before building heap buffer space).
	static void ComputeEntrySizes(Vector &v, VectorData &vdata, idx_t entry_sizes[], idx_t vcount, idx_t ser_count,
	                              const SelectionVector &sel, idx_t offset = 0);
	//! Scatter vector with variable size type to the heap.
	static void HeapScatter(Vector &v, idx_t vcount, const SelectionVector &sel, idx_t ser_count, idx_t col_idx,
	                        data_ptr_t *key_locations, data_ptr_t *validitymask_locations, idx_t offset = 0);
	//! Scatter vector data with variable size type to the heap.
	static void HeapScatterVData(VectorData &vdata, PhysicalType type, const SelectionVector &sel, idx_t ser_count,
	                             idx_t col_idx, data_ptr_t *key_locations, data_ptr_t *validitymask_locations,
	                             idx_t offset = 0);
	//! Gather a single column with variable size type from the heap.
	static void HeapGather(Vector &v, const idx_t &vcount, const SelectionVector &sel, const idx_t &col_idx,
	                       data_ptr_t key_locations[], data_ptr_t validitymask_locations[]);

	//===--------------------------------------------------------------------===//
	// Sorting Operators
	//===--------------------------------------------------------------------===//
	//! Scatter vector data to the rows in radix-sortable format.
	static void RadixScatter(Vector &v, idx_t vcount, const SelectionVector &sel, idx_t ser_count,
	                         data_ptr_t key_locations[], bool desc, bool has_null, bool nulls_first, idx_t prefix_len,
	                         idx_t width, idx_t offset = 0);

	//===--------------------------------------------------------------------===//
	// Out-of-Core Operators
	//===--------------------------------------------------------------------===//
	//! Swizzles blob pointers to offset within heap row
	static void SwizzleColumns(const RowLayout &layout, const data_ptr_t base_row_ptr, const idx_t count);
	//! Swizzles the base pointer of each row to offset within heap block
	static void SwizzleHeapPointer(const RowLayout &layout, data_ptr_t row_ptr, const data_ptr_t heap_base_ptr,
	                               const idx_t count);
	//! Unswizzles all offsets back to pointers
	static void UnswizzlePointers(const RowLayout &layout, const data_ptr_t base_row_ptr,
	                              const data_ptr_t base_heap_ptr, const idx_t count);
};

} // namespace duckdb
