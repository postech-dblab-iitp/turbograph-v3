//===--------------------------------------------------------------------===//
// row_gather.cpp
// Description: This file contains the implementation of the gather operators
//===--------------------------------------------------------------------===//

#include "common/exception.hpp"
#include "common/operator/constant_operators.hpp"
#include "common/row_operations/row_operations.hpp"
#include "common/types/row_data_collection.hpp"
#include "common/types/row_layout.hpp"

namespace duckdb {

static void print_bytes(const void *object, size_t size)
{
#ifdef __cplusplus
  const unsigned char * const bytes = static_cast<const unsigned char *>(object);
#else // __cplusplus
  const unsigned char * const bytes = object;
#endif // __cplusplus

  size_t i;

  printf("[ ");
  for(i = 0; i < size; i++)
  {
    printf("%02x ", bytes[i]);
  }
  printf("]\n");
}


using ValidityBytes = RowLayout::ValidityBytes;

template <class T>
static void TemplatedGatherLoop(Vector &rows, const SelectionVector &row_sel, Vector &col,
                                const SelectionVector &col_sel, idx_t count, idx_t col_offset, idx_t col_no,
                                idx_t build_size) {
	// Precompute mask indexes
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(col_no, entry_idx, idx_in_entry);
	auto ptrs = FlatVector::GetData<data_ptr_t>(rows);
	auto data = FlatVector::GetData<T>(col);
	auto &col_mask = FlatVector::Validity(col);

	for (idx_t i = 0; i < count; i++) {
		auto row_idx = row_sel.get_index(i);
		auto row = ptrs[row_idx];
		auto col_idx = col_sel.get_index(i);
		data[col_idx] = Load<T>(row + col_offset);
		ValidityBytes row_mask(row);
		if (!row_mask.RowIsValid(row_mask.GetValidityEntry(entry_idx), idx_in_entry)) {
			if (build_size > STANDARD_VECTOR_SIZE && col_mask.AllValid()) {
				//! We need to initialize the mask with the vector size.
				col_mask.Initialize(build_size);
			}
			col_mask.SetInvalid(col_idx);
		}
	}
}

static void GatherNestedVector(Vector &rows, const SelectionVector &row_sel, Vector &col,
                               const SelectionVector &col_sel, idx_t count, idx_t col_offset, idx_t col_no) {
	auto ptrs = FlatVector::GetData<data_ptr_t>(rows);

	// Build the gather locations
	auto data_locations = unique_ptr<data_ptr_t[]>(new data_ptr_t[count]);
	auto mask_locations = unique_ptr<data_ptr_t[]>(new data_ptr_t[count]);
	for (idx_t i = 0; i < count; i++) {
		auto row_idx = row_sel.get_index(i);
		mask_locations[i] = ptrs[row_idx];
		data_locations[i] = Load<data_ptr_t>(ptrs[row_idx] + col_offset);
	}

	// Deserialise into the selected locations
	RowOperations::HeapGather(col, count, col_sel, col_no, data_locations.get(), mask_locations.get());
}

void RowOperations::Gather(Vector &rows, const SelectionVector &row_sel, Vector &col, const SelectionVector &col_sel,
                           const idx_t count, const idx_t col_offset, const idx_t col_no, const idx_t build_size) {
	D_ASSERT(rows.GetVectorType() == VectorType::FLAT_VECTOR);
	D_ASSERT(rows.GetType().id() == LogicalTypeId::POINTER); // "Cannot gather from non-pointer type!"

	col.SetVectorType(VectorType::FLAT_VECTOR);
	switch (col.GetType().InternalType()) {
	case PhysicalType::UINT8:
		TemplatedGatherLoop<uint8_t>(rows, row_sel, col, col_sel, count, col_offset, col_no, build_size);
		break;
	case PhysicalType::UINT16:
		TemplatedGatherLoop<uint16_t>(rows, row_sel, col, col_sel, count, col_offset, col_no, build_size);
		break;
	case PhysicalType::UINT32:
		TemplatedGatherLoop<uint32_t>(rows, row_sel, col, col_sel, count, col_offset, col_no, build_size);
		break;
	case PhysicalType::UINT64:
		TemplatedGatherLoop<uint64_t>(rows, row_sel, col, col_sel, count, col_offset, col_no, build_size);
		break;
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedGatherLoop<int8_t>(rows, row_sel, col, col_sel, count, col_offset, col_no, build_size);
		break;
	case PhysicalType::INT16:
		TemplatedGatherLoop<int16_t>(rows, row_sel, col, col_sel, count, col_offset, col_no, build_size);
		break;
	case PhysicalType::INT32:
		TemplatedGatherLoop<int32_t>(rows, row_sel, col, col_sel, count, col_offset, col_no, build_size);
		break;
	case PhysicalType::INT64:
		TemplatedGatherLoop<int64_t>(rows, row_sel, col, col_sel, count, col_offset, col_no, build_size);
		break;
	case PhysicalType::INT128:
		TemplatedGatherLoop<hugeint_t>(rows, row_sel, col, col_sel, count, col_offset, col_no, build_size);
		break;
	case PhysicalType::FLOAT:
		TemplatedGatherLoop<float>(rows, row_sel, col, col_sel, count, col_offset, col_no, build_size);
		break;
	case PhysicalType::DOUBLE:
		TemplatedGatherLoop<double>(rows, row_sel, col, col_sel, count, col_offset, col_no, build_size);
		break;
	case PhysicalType::INTERVAL:
		TemplatedGatherLoop<interval_t>(rows, row_sel, col, col_sel, count, col_offset, col_no, build_size);
		break;
	case PhysicalType::VARCHAR:
		TemplatedGatherLoop<string_t>(rows, row_sel, col, col_sel, count, col_offset, col_no, build_size);
		break;
	case PhysicalType::LIST:
	case PhysicalType::MAP:
	case PhysicalType::STRUCT:
		GatherNestedVector(rows, row_sel, col, col_sel, count, col_offset, col_no);
		break;
	default:
		throw InternalException("Unimplemented type for RowOperations::Gather ("
			+ col.GetType().ToString() + "/" + TypeIdToString(col.GetType().InternalType()) + ")");
	}
}

template <class T>
static void TemplatedFullScanLoop(Vector &rows, Vector &col, idx_t count, idx_t col_offset, idx_t col_no) {
	// Precompute mask indexes
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(col_no, entry_idx, idx_in_entry);

	auto ptrs = FlatVector::GetData<data_ptr_t>(rows);
	auto data = FlatVector::GetData<T>(col);
	//	auto &col_mask = FlatVector::Validity(col);

	for (idx_t i = 0; i < count; i++) {
		auto row = ptrs[i];
		data[i] = Load<T>(row + col_offset);
		ValidityBytes row_mask(row);
		if (!row_mask.RowIsValid(row_mask.GetValidityEntry(entry_idx), idx_in_entry)) {
			throw InternalException("Null value comparisons not implemented for perfect hash table yet");
			//			col_mask.SetInvalid(i);
		}
	}
}

void RowOperations::FullScanColumn(const RowLayout &layout, Vector &rows, Vector &col, idx_t count, idx_t col_no) {
	const auto col_offset = layout.GetOffsets()[col_no];
	col.SetVectorType(VectorType::FLAT_VECTOR);
	switch (col.GetType().InternalType()) {
	case PhysicalType::UINT8:
		TemplatedFullScanLoop<uint8_t>(rows, col, count, col_offset, col_no);
		break;
	case PhysicalType::UINT16:
		TemplatedFullScanLoop<uint16_t>(rows, col, count, col_offset, col_no);
		break;
	case PhysicalType::UINT32:
		TemplatedFullScanLoop<uint32_t>(rows, col, count, col_offset, col_no);
		break;
	case PhysicalType::UINT64:
		TemplatedFullScanLoop<uint64_t>(rows, col, count, col_offset, col_no);
		break;
	case PhysicalType::INT8:
		TemplatedFullScanLoop<int8_t>(rows, col, count, col_offset, col_no);
		break;
	case PhysicalType::INT16:
		TemplatedFullScanLoop<int16_t>(rows, col, count, col_offset, col_no);
		break;
	case PhysicalType::INT32:
		TemplatedFullScanLoop<int32_t>(rows, col, count, col_offset, col_no);
		break;
	case PhysicalType::INT64:
		TemplatedFullScanLoop<int64_t>(rows, col, count, col_offset, col_no);
		break;
	default:
		throw NotImplementedException("Unimplemented type for RowOperations::FullScanColumn");
	}
}

} // namespace duckdb
