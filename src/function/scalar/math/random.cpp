#include "function/scalar/math_functions.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "execution/expression_executor.hpp"
#include "main/client_context.hpp"
#include "planner/expression/bound_function_expression.hpp"
#include <random>

namespace duckdb {

struct RandomBindData : public FunctionData {
	ClientContext &context;
	std::uniform_real_distribution<double> dist;

	RandomBindData(ClientContext &context, std::uniform_real_distribution<double> dist) : context(context), dist(dist) {
	}

	unique_ptr<FunctionData> Copy() override {
		return make_unique<RandomBindData>(context, dist);
	}
};

static void RandomFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 0);
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (RandomBindData &)*func_expr.bind_info;

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<double>(result);
	for (idx_t i = 0; i < args.size(); i++) {
		D_ASSERT(false); // TODO
		// result_data[i] = info.dist(info.context.random_engine);
	}
}

unique_ptr<FunctionData> RandomBind(ClientContext &context, ScalarFunction &bound_function,
                                    vector<unique_ptr<Expression>> &arguments) {
	std::uniform_real_distribution<double> dist(0, 1);
	return make_unique<RandomBindData>(context, dist);
}

void RandomFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("random", {}, LogicalType::DOUBLE, RandomFunction, true, RandomBind));
}

} // namespace duckdb
