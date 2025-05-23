#include "planner/expression/bound_function_expression.hpp"
#include "common/string_util.hpp"
#include "parser/expression/bound_expression.hpp"
#include "function/scalar/nested_functions.hpp"
#include "common/types/data_chunk.hpp"
#include "common/pair.hpp"
#include "storage/statistics/list_statistics.hpp"
#include "planner/expression_binder.hpp"

namespace duckdb {

static void ListValueFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);
	auto &child_type = ListType::GetChildType(result.GetType());

	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	for (idx_t i = 0; i < args.ColumnCount(); i++) {
		if (args.data[i].GetVectorType() != VectorType::CONSTANT_VECTOR) {
			result.SetVectorType(VectorType::FLAT_VECTOR);
		}
	}

	auto result_data = FlatVector::GetData<list_entry_t>(result);
	for (idx_t i = 0; i < args.size(); i++) {
		result_data[i].offset = ListVector::GetListSize(result);
		for (idx_t col_idx = 0; col_idx < args.ColumnCount(); col_idx++) {
			auto val = args.GetValue(col_idx, i).CastAs(child_type);
			ListVector::PushBack(result, val);
		}
		result_data[i].length = args.ColumnCount();
	}
	result.Verify(args.size());
}

static unique_ptr<FunctionData> ListValueBind(ClientContext &context, ScalarFunction &bound_function,
                                              vector<unique_ptr<Expression>> &arguments) {
	// collect names and deconflict, construct return type
	LogicalType child_type = LogicalType::SQLNULL;
	bound_function.arguments.resize(arguments.size());
	for (idx_t i = 0; i < arguments.size(); i++) {
		child_type = LogicalType::MaxLogicalType(child_type, arguments[i]->return_type);
		bound_function.arguments[i] = arguments[i]->return_type;
	}
	// ExpressionBinder::ResolveParameterType(child_type); // TODO

	// this is more for completeness reasons
	bound_function.varargs = child_type;
	bound_function.return_type = LogicalType::LIST(move(child_type));
	return make_unique<VariableReturnBindData>(bound_function.return_type);
}

unique_ptr<BaseStatistics> ListValueStats(ClientContext &context, BoundFunctionExpression &expr,
                                          FunctionData *bind_data, vector<unique_ptr<BaseStatistics>> &child_stats) {
	auto list_stats = make_unique<ListStatistics>(expr.return_type);
	for (idx_t i = 0; i < child_stats.size(); i++) {
		if (child_stats[i]) {
			list_stats->child_stats->Merge(*child_stats[i]);
		} else {
			list_stats->child_stats.reset();
			return move(list_stats);
		}
	}
	return move(list_stats);
}

void ListValueFun::RegisterFunction(BuiltinFunctions &set) {
	// the arguments and return types are actually set in the binder function
	ScalarFunction fun("list_value", {}, LogicalTypeId::LIST, ListValueFunction, false, ListValueBind, nullptr,
	                   ListValueStats);
	fun.varargs = LogicalType::ANY;
	set.AddFunction(fun);
	fun.name = "list_pack";
	set.AddFunction(fun);
    fun.name = "list_creation"; // for kuzu
    set.AddFunction(fun);
}

} // namespace duckdb
