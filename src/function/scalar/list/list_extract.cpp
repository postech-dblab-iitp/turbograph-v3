// #include "planner/expression/bound_function_expression.hpp"
// #include "common/string_util.hpp"
// #include "common/vector_operations/binary_executor.hpp"
// #include "parser/expression/bound_expression.hpp"
// #include "function/scalar/nested_functions.hpp"
// #include "function/scalar/string_functions.hpp"
// #include "common/types/chunk_collection.hpp"
// #include "common/types/data_chunk.hpp"
// #include "common/pair.hpp"
// #include "storage/statistics/list_statistics.hpp"
// #include "storage/statistics/validity_statistics.hpp"

// namespace duckdb {

// template <class T, bool HEAP_REF = false, bool VALIDITY_ONLY = false>
// void ListExtractTemplate(idx_t count, VectorData &list_data, VectorData &offsets_data, Vector &child_vector,
//                          idx_t list_size, Vector &result) {
// 	VectorData child_data;
// 	child_vector.Orrify(list_size, child_data);

// 	T *result_data;

// 	result.SetVectorType(VectorType::FLAT_VECTOR);
// 	if (!VALIDITY_ONLY) {
// 		result_data = FlatVector::GetData<T>(result);
// 	}
// 	auto &result_mask = FlatVector::Validity(result);

// 	// heap-ref once
// 	if (HEAP_REF) {
// 		StringVector::AddHeapReference(result, child_vector);
// 	}

// 	// this is lifted from ExecuteGenericLoop because we can't push the list child data into this otherwise
// 	// should have gone with GetValue perhaps
// 	for (idx_t i = 0; i < count; i++) {
// 		auto list_index = list_data.sel->get_index(i);
// 		auto offsets_index = offsets_data.sel->get_index(i);
// 		if (list_data.validity.RowIsValid(list_index) && offsets_data.validity.RowIsValid(offsets_index)) {
// 			auto list_entry = ((list_entry_t *)list_data.data)[list_index];
// 			auto offsets_entry = ((int64_t *)offsets_data.data)[offsets_index];

// 			// 1-based indexing
// 			if (offsets_entry == 0) {
// 				result_mask.SetInvalid(i);
// 				continue;
// 			}
// 			offsets_entry = (offsets_entry > 0) ? offsets_entry - 1 : offsets_entry;

// 			idx_t child_offset;
// 			if (offsets_entry < 0) {
// 				if ((idx_t)-offsets_entry > list_entry.length) {
// 					result_mask.SetInvalid(i);
// 					continue;
// 				}
// 				child_offset = list_entry.offset + list_entry.length + offsets_entry;
// 			} else {
// 				if ((idx_t)offsets_entry >= list_entry.length) {
// 					result_mask.SetInvalid(i);
// 					continue;
// 				}
// 				child_offset = list_entry.offset + offsets_entry;
// 			}
// 			if (child_data.validity.RowIsValid(child_offset)) {
// 				if (!VALIDITY_ONLY) {
// 					result_data[i] = ((T *)child_data.data)[child_offset];
// 				}
// 			} else {
// 				result_mask.SetInvalid(i);
// 			}
// 		} else {
// 			result_mask.SetInvalid(i);
// 		}
// 	}
// 	if (count == 1) {
// 		result.SetVectorType(VectorType::CONSTANT_VECTOR);
// 	}
// }
// static void ExecuteListExtractInternal(const idx_t count, VectorData &list, VectorData &offsets, Vector &child_vector,
//                                        idx_t list_size, Vector &result) {
// 	D_ASSERT(child_vector.GetType() == result.GetType());
// 	switch (result.GetType().InternalType()) {
// 	case PhysicalType::BOOL:
// 	case PhysicalType::INT8:
// 		ListExtractTemplate<int8_t>(count, list, offsets, child_vector, list_size, result);
// 		break;
// 	case PhysicalType::INT16:
// 		ListExtractTemplate<int16_t>(count, list, offsets, child_vector, list_size, result);
// 		break;
// 	case PhysicalType::INT32:
// 		ListExtractTemplate<int32_t>(count, list, offsets, child_vector, list_size, result);
// 		break;
// 	case PhysicalType::INT64:
// 		ListExtractTemplate<int64_t>(count, list, offsets, child_vector, list_size, result);
// 		break;
// 	case PhysicalType::INT128:
// 		ListExtractTemplate<hugeint_t>(count, list, offsets, child_vector, list_size, result);
// 		break;
// 	case PhysicalType::UINT8:
// 		ListExtractTemplate<uint8_t>(count, list, offsets, child_vector, list_size, result);
// 		break;
// 	case PhysicalType::UINT16:
// 		ListExtractTemplate<uint16_t>(count, list, offsets, child_vector, list_size, result);
// 		break;
// 	case PhysicalType::UINT32:
// 		ListExtractTemplate<uint32_t>(count, list, offsets, child_vector, list_size, result);
// 		break;
// 	case PhysicalType::UINT64:
// 		ListExtractTemplate<uint64_t>(count, list, offsets, child_vector, list_size, result);
// 		break;
// 	case PhysicalType::FLOAT:
// 		ListExtractTemplate<float>(count, list, offsets, child_vector, list_size, result);
// 		break;
// 	case PhysicalType::DOUBLE:
// 		ListExtractTemplate<double>(count, list, offsets, child_vector, list_size, result);
// 		break;
// 	case PhysicalType::VARCHAR:
// 		ListExtractTemplate<string_t, true>(count, list, offsets, child_vector, list_size, result);
// 		break;
// 	case PhysicalType::INTERVAL:
// 		ListExtractTemplate<interval_t>(count, list, offsets, child_vector, list_size, result);
// 		break;
// 	case PhysicalType::STRUCT: {
// 		auto &entries = StructVector::GetEntries(child_vector);
// 		auto &result_entries = StructVector::GetEntries(result);
// 		D_ASSERT(entries.size() == result_entries.size());
// 		// extract the child entries of the struct
// 		for (idx_t i = 0; i < entries.size(); i++) {
// 			ExecuteListExtractInternal(count, list, offsets, *entries[i], list_size, *result_entries[i]);
// 		}
// 		// extract the validity mask
// 		ListExtractTemplate<bool, false, true>(count, list, offsets, child_vector, list_size, result);
// 		break;
// 	}
// 	case PhysicalType::LIST: {
// 		// nested list: we have to reference the child
// 		auto &child_child_list = ListVector::GetEntry(child_vector);

// 		ListVector::GetEntry(result).Reference(child_child_list);
// 		ListVector::SetListSize(result, ListVector::GetListSize(child_vector));
// 		ListExtractTemplate<list_entry_t>(count, list, offsets, child_vector, list_size, result);
// 		break;
// 	}
// 	default:
// 		throw NotImplementedException("Unimplemented type for LIST_EXTRACT");
// 	}
// }

// static void ExecuteListExtract(Vector &result, Vector &list, Vector &offsets, const idx_t count) {
// 	D_ASSERT(list.GetType().id() == LogicalTypeId::LIST);
// 	VectorData list_data;
// 	VectorData offsets_data;

// 	list.Orrify(count, list_data);
// 	offsets.Orrify(count, offsets_data);
// 	ExecuteListExtractInternal(count, list_data, offsets_data, ListVector::GetEntry(list),
// 	                           ListVector::GetListSize(list), result);
// 	result.Verify(count);
// }

// static void ExecuteStringExtract(Vector &result, Vector &input_vector, Vector &subscript_vector, const idx_t count) {
// 	BinaryExecutor::Execute<string_t, int32_t, string_t>(
// 	    input_vector, subscript_vector, result, count, [&](string_t input_string, int32_t subscript) {
// 		    return SubstringFun::SubstringScalarFunction(result, input_string, subscript, 1);
// 	    });
// }

// static void ListExtractFunction(DataChunk &args, ExpressionState &state, Vector &result) {
// 	D_ASSERT(args.ColumnCount() == 2);
// 	auto count = args.size();

// 	result.SetVectorType(VectorType::CONSTANT_VECTOR);
// 	for (idx_t i = 0; i < args.ColumnCount(); i++) {
// 		if (args.data[i].GetVectorType() != VectorType::CONSTANT_VECTOR) {
// 			result.SetVectorType(VectorType::FLAT_VECTOR);
// 		}
// 	}

// 	Vector &base = args.data[0];
// 	Vector &subscript = args.data[1];

// 	switch (base.GetType().id()) {
// 	case LogicalTypeId::LIST:
// 		ExecuteListExtract(result, base, subscript, count);
// 		break;
// 	case LogicalTypeId::VARCHAR:
// 		ExecuteStringExtract(result, base, subscript, count);
// 		break;
// 	case LogicalTypeId::SQLNULL:
// 		result.SetVectorType(VectorType::CONSTANT_VECTOR);
// 		ConstantVector::SetNull(result, true);
// 		break;
// 	default:
// 		throw NotImplementedException("Specifier type not implemented");
// 	}
// }

// static unique_ptr<FunctionData> ListExtractBind(ClientContext &context, ScalarFunction &bound_function,
//                                                 vector<unique_ptr<Expression>> &arguments) {
// 	D_ASSERT(bound_function.arguments.size() == 2);
// 	if (arguments[0]->return_type.id() == LogicalTypeId::SQLNULL) {
// 		bound_function.arguments[0] = LogicalType::SQLNULL;
// 		bound_function.return_type = LogicalType::SQLNULL;
// 	} else {
// 		D_ASSERT(LogicalTypeId::LIST == arguments[0]->return_type.id());
// 		// list extract returns the child type of the list as return type
// 		bound_function.return_type = ListType::GetChildType(arguments[0]->return_type);
// 	}
// 	return make_unique<VariableReturnBindData>(bound_function.return_type);
// }

// static unique_ptr<BaseStatistics> ListExtractStats(ClientContext &context, BoundFunctionExpression &expr,
//                                                    FunctionData *bind_data,
//                                                    vector<unique_ptr<BaseStatistics>> &child_stats) {
// 	if (!child_stats[0]) {
// 		return nullptr;
// 	}
// 	auto &list_stats = (ListStatistics &)*child_stats[0];
// 	if (!list_stats.child_stats) {
// 		return nullptr;
// 	}
// 	auto child_copy = list_stats.child_stats->Copy();
// 	// list_extract always pushes a NULL, since if the offset is out of range for a list it inserts a null
// 	child_copy->validity_stats = make_unique<ValidityStatistics>(true);
// 	return child_copy;
// }

// void ListExtractFun::RegisterFunction(BuiltinFunctions &set) {
// 	// the arguments and return types are actually set in the binder function
// 	ScalarFunction lfun({LogicalType::LIST(LogicalType::ANY), LogicalType::BIGINT}, LogicalType::ANY,
// 	                    ListExtractFunction, false, false, ListExtractBind, nullptr, ListExtractStats);

// 	ScalarFunction sfun({LogicalType::VARCHAR, LogicalType::INTEGER}, LogicalType::VARCHAR, ListExtractFunction, false,
// 	                    false, nullptr);

// 	ScalarFunctionSet list_extract("list_extract");
// 	list_extract.AddFunction(lfun);
// 	list_extract.AddFunction(sfun);
// 	set.AddFunction(list_extract);

// 	ScalarFunctionSet list_element("list_element");
// 	list_element.AddFunction(lfun);
// 	list_element.AddFunction(sfun);
// 	set.AddFunction(list_element);

// 	// ScalarFunctionSet array_extract("array_extract");
// 	// array_extract.AddFunction(lfun);
// 	// array_extract.AddFunction(sfun);
// 	// array_extract.AddFunction(StructExtractFun::GetFunction());
// 	// set.AddFunction(array_extract);
// }

// } // namespace duckdb
