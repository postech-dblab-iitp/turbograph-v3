// // TODO imported from duckdb 
// #include "typedef.hpp"

// #include "execution/physical_operator/filter.hpp"

// #include <string>

// #include "execution/expression_executor.hpp"
// #include "planner/expression/bound_conjunction_expression.hpp"

// #include "duckdb/execution/operator/filter/physical_filter.hpp"

// namespace duckdb {

// class FilterState : public OperatorState {
// public:
// 	explicit FilterState(ExecutionContext &context, Expression &expr)
// 	    : executor(Allocator::Get(context.client), expr), sel(STANDARD_VECTOR_SIZE) {
// 	}
// public:

// 	ExpressionExecutor executor;
// 	SelectionVector sel;


// };

// unique_ptr<OperatorState> Filter::GetOperatorState() const {
// 	return make_unique<FilterState>(context, *expression);
// }

// Filter::Filter(CypherSchema& sch, vector<unique_ptr<Expression>> select_list)
// 	: CypherPhysicalOperator(sch) {

// 	D_ASSERT(select_list.size() > 0);
// 	if (select_list.size() > 1) {
// 		// // create a big AND out of the expressions
// 		// auto conjunction = make_unique<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
// 		// for (auto &expr : select_list) {
// 		// 	conjunction->children.push_back(move(expr));
// 		// }
// 		// expression = move(conjunction);
// 		D_ASSERT(false);
// 	} else {
// 		expression = move(select_list[0]);
// 	}

// }

// OperatorResultType Filter::Execute(GraphStore* graph, DataChunk &input, DataChunk &chunk, OperatorState &state) const {
// 	//auto &state = (FilterState &)state;
// 	// state not necessary

// 	// seek input using targetindex.
// 	auto &state = (FilterState &)state_p;
// 	idx_t result_count = state.executor.SelectExpression(input, state.sel);
// 	if (result_count == input.size()) {
// 		// nothing was filtered: skip adding any selection vectors
// 		chunk.Reference(input);
// 	} else {
// 		chunk.Slice(input, state.sel, result_count);
// 	}
// 	return OperatorResultType::NEED_MORE_INPUT;

// }


// std::string Filter::ParamsToString() const {
// 	return "filter-param";
// }

// std::string Filter::ToString() const {
// 	return "Filter";
// }

// }