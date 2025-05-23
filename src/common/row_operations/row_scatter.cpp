//===--------------------------------------------------------------------===//
// row_scatter.cpp
// Description: This file contains the implementation of the row scattering
//              operators
//===--------------------------------------------------------------------===//

#include "common/exception.hpp"
#include "common/helper.hpp"
#include "common/row_operations/row_operations.hpp"
#include "common/types/null_value.hpp"
#include "common/types/row_data_collection.hpp"
#include "common/types/row_layout.hpp"
#include "common/types/selection_vector.hpp"
#include "common/types/vector.hpp"

#include "icecream.hpp"

namespace duckdb {

using ValidityBytes = RowLayout::ValidityBytes;

template <class T>
static void TemplatedScatter(VectorData &col, Vector &rows, const SelectionVector &sel, const idx_t count,
                             const idx_t col_offset, const idx_t col_no) {
	auto data = (T *)col.data;
	auto ptrs = FlatVector::GetData<data_ptr_t>(rows);

	if (!col.is_valid) {
		for (idx_t i = 0; i < count; i++) {
			auto idx = sel.get_index(i);
			auto row = ptrs[idx];

			T store_value = NullValue<T>();
			Store<T>(store_value, row + col_offset);
			ValidityBytes col_mask(ptrs[idx]);
			col_mask.SetInvalidUnsafe(col_no);
		}
	} else {
		if (!col.validity.AllValid()) {
			for (idx_t i = 0; i < count; i++) {
				auto idx = sel.get_index(i);
				auto col_idx = col.sel->get_index(idx);
				auto row = ptrs[idx];

				auto isnull = !col.validity.RowIsValid(col_idx);
				T store_value = isnull ? NullValue<T>() : data[col_idx];
				Store<T>(store_value, row + col_offset);
				if (isnull) {
					ValidityBytes col_mask(ptrs[idx]);
					col_mask.SetInvalidUnsafe(col_no);
				}
			}
		} else {
			for (idx_t i = 0; i < count; i++) {
				auto idx = sel.get_index(i);
				auto col_idx = col.sel->get_index(idx);
				auto row = ptrs[idx];

				Store<T>(data[col_idx], row + col_offset);
			}
		}
	}
}

static void ComputeStringEntrySizes(const VectorData &col, idx_t entry_sizes[], const SelectionVector &sel,
                                    const idx_t count, const idx_t offset = 0) {
	auto data = (const string_t *)col.data;
	if (!col.is_valid) {
		return;
	} else {
		for (idx_t i = 0; i < count; i++) {
			auto idx = sel.get_index(i);
			auto col_idx = col.sel->get_index(idx) + offset;
			const auto &str = data[col_idx];
			if (col.validity.RowIsValid(col_idx) && col.is_valid && !str.IsInlined()) {
				entry_sizes[i] += str.GetSize();
			}
		}
	}
}

static void ScatterStringVector(VectorData &col, Vector &rows, data_ptr_t str_locations[], const SelectionVector &sel,
                                const idx_t count, const idx_t col_offset, const idx_t col_no) {
	auto string_data = (string_t *)col.data;
	auto ptrs = FlatVector::GetData<data_ptr_t>(rows);

	if (!col.is_valid) {
		for (idx_t i = 0; i < count; i++) {
			auto idx = sel.get_index(i);
			auto row = ptrs[idx];
			ValidityBytes col_mask(row);
			col_mask.SetInvalidUnsafe(col_no);
			Store<string_t>(NullValue<string_t>(), row + col_offset);
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			auto idx = sel.get_index(i);
			auto col_idx = col.sel->get_index(idx);
			auto row = ptrs[idx];
			if (!col.validity.RowIsValid(col_idx) || !col.is_valid) {
				ValidityBytes col_mask(row);
				col_mask.SetInvalidUnsafe(col_no);
				Store<string_t>(NullValue<string_t>(), row + col_offset);
			} else if (string_data[col_idx].IsInlined()) {
				Store<string_t>(string_data[col_idx], row + col_offset);
			} else {
				const auto &str = string_data[col_idx];
				string_t inserted((const char *)str_locations[i], str.GetSize());
				memcpy(inserted.GetDataWriteable(), str.GetDataUnsafe(), str.GetSize());
				str_locations[i] += str.GetSize();
				inserted.Finalize();
				Store<string_t>(inserted, row + col_offset);
			}
		}
	}
}

static void ScatterNestedVector(Vector &vec, VectorData &col, Vector &rows, data_ptr_t data_locations[],
                                const SelectionVector &sel, const idx_t count, const idx_t col_offset,
                                const idx_t col_no, const idx_t vcount) {
	// Store pointers to the data in the row
	// Do this first because SerializeVector destroys the locations
	auto ptrs = FlatVector::GetData<data_ptr_t>(rows);
	data_ptr_t validitymask_locations[EXEC_ENGINE_VECTOR_SIZE];
	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		auto row = ptrs[idx];
		validitymask_locations[i] = row;

		Store<data_ptr_t>(data_locations[i], row + col_offset);
	}

	// Serialise the data
	RowOperations::HeapScatter(vec, vcount, sel, count, col_no, data_locations, validitymask_locations);
}

void RowOperations::Scatter(DataChunk &columns, VectorData col_data[], const RowLayout &layout, Vector &rows,
                            RowDataCollection &string_heap, const SelectionVector &sel, idx_t count) {
	if (count == 0) {
		return;
	}

	// Set the validity mask for each row before inserting data
	auto ptrs = FlatVector::GetData<data_ptr_t>(rows);
	for (idx_t i = 0; i < count; ++i) {
		auto row_idx = sel.get_index(i);
		auto row = ptrs[row_idx];
		ValidityBytes(row).SetAllValid(layout.ColumnCount());
	}

	const auto vcount = columns.size();
	auto &offsets = layout.GetOffsets();
	auto &types = layout.GetTypes();

	// Compute the entry size of the variable size columns
	vector<unique_ptr<BufferHandle>> handles;
	data_ptr_t data_locations[EXEC_ENGINE_VECTOR_SIZE];

	// scatter for non-constant-sized values. here, swizlling is performed.
	if (!layout.AllConstant()) {
		idx_t entry_sizes[EXEC_ENGINE_VECTOR_SIZE];
		std::fill_n(entry_sizes, count, sizeof(uint32_t));
		for (idx_t col_no = 0; col_no < types.size(); col_no++) {
			if (TypeIsConstantSize(types[col_no].InternalType())) {
				continue;
			}
			// only non-constant columns

			auto &vec = columns.data[col_no];
			auto &col = col_data[col_no];
			switch (types[col_no].InternalType()) {
			case PhysicalType::VARCHAR:
				ComputeStringEntrySizes(col, entry_sizes, sel, count);
				break;
			case PhysicalType::LIST:
			case PhysicalType::MAP:
			case PhysicalType::STRUCT:
				RowOperations::ComputeEntrySizes(vec, col, entry_sizes, vcount, count, sel);
				break;
			default:
				throw InternalException("Unsupported type for RowOperations::Scatter");
			}
		}

		// Build out the buffer space
		string_heap.Build(count, data_locations, entry_sizes);

		// Serialize information that is needed for swizzling if the computation goes out-of-core
		const idx_t heap_pointer_offset = layout.GetHeapPointerOffset();
		for (idx_t i = 0; i < count; i++) {
			auto row_idx = sel.get_index(i);
			auto row = ptrs[row_idx];
			// Pointer to this row in the heap block
			Store<data_ptr_t>(data_locations[i], row + heap_pointer_offset);
			// Row size is stored in the heap in front of each row
			Store<uint32_t>(entry_sizes[i], data_locations[i]);
			data_locations[i] += sizeof(uint32_t);
		}
	}

	for (idx_t col_no = 0; col_no < types.size(); col_no++) {
		auto &vec = columns.data[col_no];
		auto &col = col_data[col_no];
		auto col_offset = offsets[col_no];

		switch (types[col_no].InternalType()) {
		case PhysicalType::BOOL:
		case PhysicalType::INT8:
			TemplatedScatter<int8_t>(col, rows, sel, count, col_offset, col_no);
			break;
		case PhysicalType::INT16:
			TemplatedScatter<int16_t>(col, rows, sel, count, col_offset, col_no);
			break;
		case PhysicalType::INT32:
			TemplatedScatter<int32_t>(col, rows, sel, count, col_offset, col_no);
			break;
		case PhysicalType::INT64:
			TemplatedScatter<int64_t>(col, rows, sel, count, col_offset, col_no);
			break;
		case PhysicalType::UINT8:
			TemplatedScatter<uint8_t>(col, rows, sel, count, col_offset, col_no);
			break;
		case PhysicalType::UINT16:
			TemplatedScatter<uint16_t>(col, rows, sel, count, col_offset, col_no);
			break;
		case PhysicalType::UINT32:
			TemplatedScatter<uint32_t>(col, rows, sel, count, col_offset, col_no);
			break;
		case PhysicalType::UINT64:
			TemplatedScatter<uint64_t>(col, rows, sel, count, col_offset, col_no);
			break;
		case PhysicalType::INT128:
			TemplatedScatter<hugeint_t>(col, rows, sel, count, col_offset, col_no);
			break;
		case PhysicalType::FLOAT:
			TemplatedScatter<float>(col, rows, sel, count, col_offset, col_no);
			break;
		case PhysicalType::DOUBLE:
			TemplatedScatter<double>(col, rows, sel, count, col_offset, col_no);
			break;
		case PhysicalType::INTERVAL:
			TemplatedScatter<interval_t>(col, rows, sel, count, col_offset, col_no);
			break;
		case PhysicalType::VARCHAR:
			ScatterStringVector(col, rows, data_locations, sel, count, col_offset, col_no);
			break;
		case PhysicalType::LIST:
		case PhysicalType::MAP:
		case PhysicalType::STRUCT:
			ScatterNestedVector(vec, col, rows, data_locations, sel, count, col_offset, col_no, vcount);
			break;
		default:
			throw InternalException("Unsupported type for RowOperations::Scatter");
		}

	}
}

} // namespace duckdb
