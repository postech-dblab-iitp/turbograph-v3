//===--------------------------------------------------------------------===//
// copy.cpp
// Description: This file contains the implementation of the different copy
// functions
//===--------------------------------------------------------------------===//

#include "common/exception.hpp"
#include "common/types/null_value.hpp"
#include "common/types/rowcol_type.hpp"
#include "common/typedef.hpp"
//#include "common/types/chunk_collection.hpp"

#include "common/vector_operations/vector_operations.hpp"

namespace duckdb {

template <class T>
static void TemplatedCopy(const Vector &source, const SelectionVector &sel, Vector &target, idx_t source_offset,
                          idx_t target_offset, idx_t copy_count) {
	auto ldata = FlatVector::GetData<T>(source);
	auto tdata = FlatVector::GetData<T>(target);
	for (idx_t i = 0; i < copy_count; i++) {
		auto source_idx = sel.get_index(source_offset + i);
		tdata[target_offset + i] = ldata[source_idx];
	}
}

template <class T>
static void TemplatedCopyRowStore(const Vector &source, const SelectionVector &sel, Vector &target, idx_t source_offset,
                          idx_t target_offset, idx_t copy_count) {
	// source data
	auto ldata = FlatVector::GetData<rowcol_t>(source);
	auto rowcol_idx = source.GetRowColIdx();
	auto *row_buffer = (VectorRowStoreBuffer *)(source.GetAuxiliary().get());
	char *row_data = row_buffer->GetRowData();

	// target data
	auto tdata = FlatVector::GetData<T>(target);
	auto &target_validity = FlatVector::Validity(target);

	D_ASSERT(rowcol_idx >= 0);

	for (idx_t i = 0; i < copy_count; i++) {
		auto source_idx = sel.get_index(source_offset + i);
		auto base_offset = ldata[source_idx].offset;
		PartialSchema *schema_ptr = (PartialSchema *)ldata[source_idx].schema_ptr;
		if (schema_ptr->hasIthCol(rowcol_idx)) {
			auto offset = schema_ptr->getIthColOffset(rowcol_idx);
			memcpy(tdata + target_offset + i,
				row_data + base_offset + offset,
				sizeof(T));
		} else {
			target_validity.SetInvalid(target_offset + i);
		}
	}
}

void VectorOperations::Copy(const Vector &source, Vector &target, const SelectionVector &sel_p, idx_t source_count,
                            idx_t source_offset, idx_t target_offset) {
	D_ASSERT(source_offset <= source_count);
	// D_ASSERT(source.GetType() == target.GetType()); // TODO right?
	D_ASSERT(source.GetType().InternalType() == target.GetType().InternalType());
	idx_t copy_count = source_count - source_offset;

	if (!source.GetIsValid() && source_offset == 0) {
		target.SetIsValid(false);
		return;
	}

	SelectionVector owned_sel;
	const SelectionVector *sel = &sel_p;
	switch (source.GetVectorType()) {
	case VectorType::DICTIONARY_VECTOR: {
		// dictionary vector: merge selection vectors
		auto &child = DictionaryVector::Child(source);
		auto &dict_sel = DictionaryVector::SelVector(source);
		// merge the selection vectors and verify the child
		auto new_buffer = dict_sel.Slice(*sel, source_count);
		SelectionVector merged_sel(new_buffer);
		VectorOperations::Copy(child, target, merged_sel, source_count, source_offset, target_offset);
		return;
	}
	case VectorType::SEQUENCE_VECTOR: {
		int64_t start, increment;
		Vector seq(source.GetType());
		SequenceVector::GetSequence(source, start, increment);
		VectorOperations::GenerateSequence(seq, source_count, *sel, start, increment);
		VectorOperations::Copy(seq, target, *sel, source_count, source_offset, target_offset);
		return;
	}
	case VectorType::CONSTANT_VECTOR:
		sel = ConstantVector::ZeroSelectionVector(copy_count, owned_sel);
		break; // carry on with below code
	case VectorType::FLAT_VECTOR:
		break;
	case VectorType::ROW_VECTOR:
		VectorOperations::CopyRowStore(source, target, sel_p, source_count, source_offset, target_offset);
		return;
	default:
		throw NotImplementedException("FIXME unimplemented vector type for VectorOperations::Copy");
	}

	if (copy_count == 0) {
		return;
	}

	// Allow copying of a single value to constant vectors
	const auto target_vector_type = target.GetVectorType();
	if (copy_count == 1 && target_vector_type == VectorType::CONSTANT_VECTOR) {
		target_offset = 0;
		target.SetVectorType(VectorType::FLAT_VECTOR);
	}
	D_ASSERT(target.GetVectorType() == VectorType::FLAT_VECTOR);

	// first copy the nullmask
	auto &tmask = FlatVector::Validity(target);
	if (source.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		const bool valid = !ConstantVector::IsNull(source);
		for (idx_t i = 0; i < copy_count; i++) {
			tmask.Set(target_offset + i, valid);
		}
	} else {
		auto &smask = FlatVector::Validity(source);
		if (smask.IsMaskSet()) {
			for (idx_t i = 0; i < copy_count; i++) {
				auto idx = sel->get_index(source_offset + i);
				tmask.Set(target_offset + i, smask.RowIsValid(idx));
			}
		}
	}

	D_ASSERT(sel);

	// now copy over the data
	switch (source.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedCopy<int8_t>(source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::INT16:
		TemplatedCopy<int16_t>(source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::INT32:
		TemplatedCopy<int32_t>(source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::INT64:
		TemplatedCopy<int64_t>(source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::UINT8:
		TemplatedCopy<uint8_t>(source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::UINT16:
		TemplatedCopy<uint16_t>(source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::UINT32:
		TemplatedCopy<uint32_t>(source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::UINT64:
		TemplatedCopy<uint64_t>(source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::INT128:
		TemplatedCopy<hugeint_t>(source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::FLOAT:
		TemplatedCopy<float>(source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::DOUBLE:
		TemplatedCopy<double>(source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::INTERVAL:
		TemplatedCopy<interval_t>(source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::VARCHAR: {
		auto ldata = FlatVector::GetData<string_t>(source);
		auto tdata = FlatVector::GetData<string_t>(target);
		for (idx_t i = 0; i < copy_count; i++) {
			auto source_idx = sel->get_index(source_offset + i);
			auto target_idx = target_offset + i;
			if (tmask.RowIsValid(target_idx)) {
				tdata[target_idx] = StringVector::AddStringOrBlob(target, ldata[source_idx]);
			}
		}
		break;
	}
	case PhysicalType::STRUCT: {
		auto &source_children = StructVector::GetEntries(source);
		auto &target_children = StructVector::GetEntries(target);
		D_ASSERT(source_children.size() == target_children.size());
		for (idx_t i = 0; i < source_children.size(); i++) {
			VectorOperations::Copy(*source_children[i], *target_children[i], *sel, source_count, source_offset,
			                       target_offset);
		}
		break;
	}
	case PhysicalType::LIST: {
		D_ASSERT(target.GetType().InternalType() == PhysicalType::LIST);

		auto &source_child = ListVector::GetEntry(source);
		auto sdata = FlatVector::GetData<list_entry_t>(source);
		auto tdata = FlatVector::GetData<list_entry_t>(target);

		if (target_vector_type == VectorType::CONSTANT_VECTOR) {
			// If we are only writing one value, then the copied values (if any) are contiguous
			// and we can just Append from the offset position
			if (!tmask.RowIsValid(target_offset)) {
				break;
			}
			auto source_idx = sel->get_index(source_offset);
			auto &source_entry = sdata[source_idx];
			const idx_t source_child_size = source_entry.length + source_entry.offset;

			//! overwrite constant target vectors.
			ListVector::SetListSize(target, 0);
			ListVector::Append(target, source_child, source_child_size, source_entry.offset);

			auto &target_entry = tdata[target_offset];
			target_entry.length = source_entry.length;
			target_entry.offset = 0;
		} else {
			//! if the source has list offsets, we need to append them to the target
			//! build a selection vector for the copied child elements
			vector<sel_t> child_rows;
			for (idx_t i = 0; i < copy_count; ++i) {
				if (tmask.RowIsValid(target_offset + i)) {
					auto source_idx = sel->get_index(source_offset + i);
					auto &source_entry = sdata[source_idx];
					for (idx_t j = 0; j < source_entry.length; ++j) {
						child_rows.emplace_back(source_entry.offset + j);
					}
				}
			}
			idx_t source_child_size = child_rows.size();
			SelectionVector child_sel(child_rows.data());

			idx_t old_target_child_len = ListVector::GetListSize(target);

			//! append to list itself
			ListVector::Append(target, source_child, child_sel, source_child_size);

			//! now write the list offsets
			for (idx_t i = 0; i < copy_count; i++) {
				auto source_idx = sel->get_index(source_offset + i);
				auto &source_entry = sdata[source_idx];
				auto &target_entry = tdata[target_offset + i];

				target_entry.length = source_entry.length;
				target_entry.offset = old_target_child_len;
				if (tmask.RowIsValid(target_offset + i)) {
					old_target_child_len += target_entry.length;
				}
			}
		}
		break;
	}
	default:
		throw NotImplementedException("Unimplemented type '%s' for copy!",
		                              TypeIdToString(source.GetType().InternalType()));
	}

	if (target_vector_type != VectorType::FLAT_VECTOR) {
		target.SetVectorType(target_vector_type);
	}
}

void VectorOperations::CopyRowStore(const Vector &source, Vector &target, const SelectionVector &sel_p, idx_t source_count,
                            idx_t source_offset, idx_t target_offset) {
	D_ASSERT(source_offset <= source_count);
	D_ASSERT(source.GetType().InternalType() == target.GetType().InternalType());
	idx_t copy_count = source_count - source_offset;

	SelectionVector owned_sel;
	const SelectionVector *sel = &sel_p;

	if (copy_count == 0) {
		return;
	}

	if (!source.GetIsValid() && source_offset == 0) {
		target.SetIsValid(false);
		return;
	}

	// Allow copying of a single value to constant vectors
	const auto target_vector_type = target.GetVectorType();
	// if (copy_count == 1 && target_vector_type == VectorType::CONSTANT_VECTOR) {
	// 	target_offset = 0;
	// 	target.SetVectorType(VectorType::FLAT_VECTOR);
	// }
	// D_ASSERT(target.GetVectorType() == VectorType::FLAT_VECTOR);

	// first copy the nullmask
	auto &tmask = FlatVector::Validity(target);
	if (source.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		D_ASSERT(false); // unacceptable situation
		// const bool valid = !ConstantVector::IsNull(source);
		// for (idx_t i = 0; i < copy_count; i++) {
		// 	tmask.Set(target_offset + i, valid);
		// }
	} else {
		auto &smask = FlatVector::Validity(source);
		if (smask.IsMaskSet()) {
			for (idx_t i = 0; i < copy_count; i++) {
				auto idx = sel->get_index(source_offset + i);
				tmask.Set(target_offset + i, smask.RowIsValid(idx));
			}
		}
	}

	D_ASSERT(sel);

	// now copy over the data
	switch (source.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedCopyRowStore<int8_t>(source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::INT16:
		TemplatedCopyRowStore<int16_t>(source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::INT32:
		TemplatedCopyRowStore<int32_t>(source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::INT64:
		TemplatedCopyRowStore<int64_t>(source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::UINT8:
		TemplatedCopyRowStore<uint8_t>(source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::UINT16:
		TemplatedCopyRowStore<uint16_t>(source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::UINT32:
		TemplatedCopyRowStore<uint32_t>(source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::UINT64:
		TemplatedCopyRowStore<uint64_t>(source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::INT128:
		TemplatedCopyRowStore<hugeint_t>(source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::FLOAT:
		TemplatedCopyRowStore<float>(source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::DOUBLE:
		TemplatedCopyRowStore<double>(source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::INTERVAL:
		TemplatedCopyRowStore<interval_t>(source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::VARCHAR: {
		// source data
		auto ldata = FlatVector::GetData<rowcol_t>(source);
		auto rowcol_idx = source.GetRowColIdx();
		auto *row_buffer = (VectorRowStoreBuffer *)(source.GetAuxiliary().get());
		char *row_data = row_buffer->GetRowData();

		// target data
		auto tdata = FlatVector::GetData<string_t>(target);
		auto &target_validity = FlatVector::Validity(target);

		for (idx_t i = 0; i < copy_count; i++) {
			auto source_idx = sel->get_index(source_offset + i);
			auto base_offset = ldata[source_idx].offset;
			PartialSchema *schema_ptr = (PartialSchema *)ldata[source_idx].schema_ptr;
			if (schema_ptr->hasIthCol(rowcol_idx)) {
				auto offset = schema_ptr->getIthColOffset(rowcol_idx);
				string_t str = *((string_t *)(row_data + base_offset + offset));
				tdata[target_offset + i] = StringVector::AddStringOrBlob(target, str);
			} else {
				target_validity.SetInvalid(target_offset + i);
			}
		}
		break;
	}
	case PhysicalType::STRUCT: {
		throw NotImplementedException("CopyRowStore");
		// auto &source_children = StructVector::GetEntries(source);
		// auto &target_children = StructVector::GetEntries(target);
		// D_ASSERT(source_children.size() == target_children.size());
		// for (idx_t i = 0; i < source_children.size(); i++) {
		// 	VectorOperations::Copy(*source_children[i], *target_children[i], *sel, source_count, source_offset,
		// 	                       target_offset);
		// }
		break;
	}
	case PhysicalType::LIST: {
		throw NotImplementedException("CopyRowStore");
		// D_ASSERT(target.GetType().InternalType() == PhysicalType::LIST);

		// auto &source_child = ListVector::GetEntry(source);
		// auto sdata = FlatVector::GetData<list_entry_t>(source);
		// auto tdata = FlatVector::GetData<list_entry_t>(target);

		// if (target_vector_type == VectorType::CONSTANT_VECTOR) {
		// 	// If we are only writing one value, then the copied values (if any) are contiguous
		// 	// and we can just Append from the offset position
		// 	if (!tmask.RowIsValid(target_offset)) {
		// 		break;
		// 	}
		// 	auto source_idx = sel->get_index(source_offset);
		// 	auto &source_entry = sdata[source_idx];
		// 	const idx_t source_child_size = source_entry.length + source_entry.offset;

		// 	//! overwrite constant target vectors.
		// 	ListVector::SetListSize(target, 0);
		// 	ListVector::Append(target, source_child, source_child_size, source_entry.offset);

		// 	auto &target_entry = tdata[target_offset];
		// 	target_entry.length = source_entry.length;
		// 	target_entry.offset = 0;
		// } else {
		// 	//! if the source has list offsets, we need to append them to the target
		// 	//! build a selection vector for the copied child elements
		// 	vector<sel_t> child_rows;
		// 	for (idx_t i = 0; i < copy_count; ++i) {
		// 		if (tmask.RowIsValid(target_offset + i)) {
		// 			auto source_idx = sel->get_index(source_offset + i);
		// 			auto &source_entry = sdata[source_idx];
		// 			for (idx_t j = 0; j < source_entry.length; ++j) {
		// 				child_rows.emplace_back(source_entry.offset + j);
		// 			}
		// 		}
		// 	}
		// 	idx_t source_child_size = child_rows.size();
		// 	SelectionVector child_sel(child_rows.data());

		// 	idx_t old_target_child_len = ListVector::GetListSize(target);

		// 	//! append to list itself
		// 	ListVector::Append(target, source_child, child_sel, source_child_size);

		// 	//! now write the list offsets
		// 	for (idx_t i = 0; i < copy_count; i++) {
		// 		auto source_idx = sel->get_index(source_offset + i);
		// 		auto &source_entry = sdata[source_idx];
		// 		auto &target_entry = tdata[target_offset + i];

		// 		target_entry.length = source_entry.length;
		// 		target_entry.offset = old_target_child_len;
		// 		if (tmask.RowIsValid(target_offset + i)) {
		// 			old_target_child_len += target_entry.length;
		// 		}
		// 	}
		// }
		break;
	}
	default:
		throw NotImplementedException("Unimplemented type '%s' for copy!",
		                              TypeIdToString(source.GetType().InternalType()));
	}

	// if (target_vector_type != VectorType::FLAT_VECTOR) {
	// 	target.SetVectorType(target_vector_type);
	// }
	// target.SetVectorType(VectorType::ROW_VECTOR);
	target.SetVectorType(VectorType::FLAT_VECTOR);
}

void VectorOperations::Copy(const Vector &source, Vector &target, idx_t source_count, idx_t source_offset,
                            idx_t target_offset) {
	switch (source.GetVectorType()) {
	case VectorType::DICTIONARY_VECTOR: {
		// dictionary: continue into child with selection vector
		auto &child = DictionaryVector::Child(source);
		auto &dict_sel = DictionaryVector::SelVector(source);
		VectorOperations::Copy(child, target, dict_sel, source_count, source_offset, target_offset);
		break;
	}
	case VectorType::CONSTANT_VECTOR: {
		SelectionVector owned_sel;
		auto sel = ConstantVector::ZeroSelectionVector(source_count, owned_sel);
		VectorOperations::Copy(source, target, *sel, source_count, source_offset, target_offset);
		break;
	}
	case VectorType::FLAT_VECTOR: {
		SelectionVector owned_sel;
		auto sel = FlatVector::IncrementalSelectionVector(source_count, owned_sel);
		VectorOperations::Copy(source, target, *sel, source_count, source_offset, target_offset);
		break;
	}
	case VectorType::SEQUENCE_VECTOR: {
		int64_t start, increment;
		SequenceVector::GetSequence(source, start, increment);
		Vector flattened(source.GetType());
		VectorOperations::GenerateSequence(flattened, source_count, start, increment);

		SelectionVector owned_sel;
		auto sel = FlatVector::IncrementalSelectionVector(source_count, owned_sel);
		VectorOperations::Copy(flattened, target, *sel, source_count, source_offset, target_offset);
		break;
	}
	case VectorType::ROW_VECTOR: {
		SelectionVector owned_sel;
		auto sel = FlatVector::IncrementalSelectionVector(source_count, owned_sel);
		VectorOperations::CopyRowStore(source, target, *sel, source_count, source_offset, target_offset);
		break;
	}
	default:
		throw NotImplementedException("FIXME: unimplemented vector type for VectorOperations::Copy");
	}
}

} // namespace duckdb
