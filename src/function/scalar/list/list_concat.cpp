// #include "common/types/data_chunk.hpp"
// #include "function/scalar/nested_functions.hpp"
// #include "planner/expression/bound_function_expression.hpp"
// #include "planner/expression_binder.hpp"
// #include "storage/statistics/list_statistics.hpp"
// #include "storage/statistics/validity_statistics.hpp"

// namespace duckdb {

// static void ListConcatFunction(DataChunk &args, ExpressionState &state, Vector &result) {
// 	D_ASSERT(args.ColumnCount() == 2);
// 	auto count = args.size();

// 	Vector &lhs = args.data[0];
// 	Vector &rhs = args.data[1];
// 	if (lhs.GetType().id() == LogicalTypeId::SQLNULL) {
// 		result.Reference(rhs);
// 		return;
// 	}
// 	if (rhs.GetType().id() == LogicalTypeId::SQLNULL) {
// 		result.Reference(lhs);
// 		return;
// 	}

// 	VectorData lhs_data;
// 	VectorData rhs_data;
// 	lhs.Orrify(count, lhs_data);
// 	rhs.Orrify(count, rhs_data);
// 	auto lhs_entries = (list_entry_t *)lhs_data.data;
// 	auto rhs_entries = (list_entry_t *)rhs_data.data;

// 	auto lhs_list_size = ListVector::GetListSize(lhs);
// 	auto rhs_list_size = ListVector::GetListSize(rhs);
// 	auto &lhs_child = ListVector::GetEntry(lhs);
// 	auto &rhs_child = ListVector::GetEntry(rhs);
// 	VectorData lhs_child_data;
// 	VectorData rhs_child_data;
// 	lhs_child.Orrify(lhs_list_size, lhs_child_data);
// 	rhs_child.Orrify(rhs_list_size, rhs_child_data);

// 	result.SetVectorType(VectorType::FLAT_VECTOR);
// 	auto result_entries = FlatVector::GetData<list_entry_t>(result);
// 	auto &result_validity = FlatVector::Validity(result);

// 	idx_t offset = 0;
// 	for (idx_t i = 0; i < count; i++) {
// 		auto lhs_list_index = lhs_data.sel->get_index(i);
// 		auto rhs_list_index = rhs_data.sel->get_index(i);
// 		if (!lhs_data.validity.RowIsValid(lhs_list_index) && !rhs_data.validity.RowIsValid(rhs_list_index)) {
// 			result_validity.SetInvalid(i);
// 			continue;
// 		}
// 		result_entries[i].offset = offset;
// 		result_entries[i].length = 0;
// 		if (lhs_data.validity.RowIsValid(lhs_list_index)) {
// 			const auto &lhs_entry = lhs_entries[lhs_list_index];
// 			result_entries[i].length += lhs_entry.length;
// 			ListVector::Append(result, lhs_child, *lhs_child_data.sel, lhs_entry.offset + lhs_entry.length,
// 			                   lhs_entry.offset);
// 		}
// 		if (rhs_data.validity.RowIsValid(rhs_list_index)) {
// 			const auto &rhs_entry = rhs_entries[rhs_list_index];
// 			result_entries[i].length += rhs_entry.length;
// 			ListVector::Append(result, rhs_child, *rhs_child_data.sel, rhs_entry.offset + rhs_entry.length,
// 			                   rhs_entry.offset);
// 		}
// 		offset += result_entries[i].length;
// 	}
// 	D_ASSERT(ListVector::GetListSize(result) == offset);

// 	if (lhs.GetVectorType() == VectorType::CONSTANT_VECTOR && rhs.GetVectorType() == VectorType::CONSTANT_VECTOR) {
// 		result.SetVectorType(VectorType::CONSTANT_VECTOR);
// 	}
// }

// static unique_ptr<FunctionData> ListConcatBind(ClientContext &context, ScalarFunction &bound_function,
//                                                vector<unique_ptr<Expression>> &arguments) {
// 	D_ASSERT(bound_function.arguments.size() == 2);

// 	auto &lhs = arguments[0]->return_type;
// 	auto &rhs = arguments[1]->return_type;
// 	if (lhs.id() == LogicalTypeId::SQLNULL && rhs.id() == LogicalTypeId::SQLNULL) {
// 		bound_function.return_type = LogicalType::SQLNULL;
// 	} else if (lhs.id() == LogicalTypeId::SQLNULL || rhs.id() == LogicalTypeId::SQLNULL) {
// 		// we mimic postgres behaviour: list_concat(NULL, my_list) = my_list
// 		bound_function.arguments[0] = lhs;
// 		bound_function.arguments[1] = rhs;
// 		bound_function.return_type = rhs.id() == LogicalTypeId::SQLNULL ? lhs : rhs;
// 	} else {
// 		D_ASSERT(lhs.id() == LogicalTypeId::LIST);
// 		D_ASSERT(rhs.id() == LogicalTypeId::LIST);

// 		// Resolve list type
// 		LogicalType child_type = LogicalType::SQLNULL;
// 		for (const auto &argument : arguments) {
// 			child_type = LogicalType::MaxLogicalType(child_type, ListType::GetChildType(argument->return_type));
// 		}
// 		ExpressionBinder::ResolveParameterType(child_type);
// 		auto list_type = LogicalType::LIST(move(child_type));

// 		bound_function.arguments[0] = list_type;
// 		bound_function.arguments[1] = list_type;
// 		bound_function.return_type = list_type;
// 	}
// 	return make_unique<VariableReturnBindData>(bound_function.return_type);
// }

// static unique_ptr<BaseStatistics> ListConcatStats(ClientContext &context, BoundFunctionExpression &expr,
//                                                   FunctionData *bind_data,
//                                                   vector<unique_ptr<BaseStatistics>> &child_stats) {
// 	D_ASSERT(child_stats.size() == 2);
// 	if (!child_stats[0] || !child_stats[1]) {
// 		return nullptr;
// 	}

// 	auto &left_stats = (ListStatistics &)*child_stats[0];
// 	auto &right_stats = (ListStatistics &)*child_stats[1];

// 	auto stats = left_stats.Copy();
// 	stats->Merge(right_stats);

// 	return stats;
// }

// ScalarFunction ListConcatFun::GetFunction() {
// 	// the arguments and return types are actually set in the binder function
// 	return ScalarFunction({LogicalType::LIST(LogicalType::ANY), LogicalType::LIST(LogicalType::ANY)},
// 	                      LogicalType::LIST(LogicalType::ANY), ListConcatFunction, false, false, ListConcatBind,
// 	                      nullptr, ListConcatStats);
// }

// void ListConcatFun::RegisterFunction(BuiltinFunctions &set) {
// 	set.AddFunction({"list_concat", "list_cat", "array_concat", "array_cat"}, GetFunction());
// }

// } // namespace duckdb
