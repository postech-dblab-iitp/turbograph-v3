// #include "execution/physical_operator/physical_sort.hpp"

// #include "planner/expression.hpp"
// #include "planner/bound_result_modifier.hpp"

// namespace duckdb {

// PhysicalSort::PhysicalSort(CypherSchema& sch, vector<BoundOrderByNode> order_exprs):
// 	CypherPhysicalOperator(sch), order_exprs(std::move(order_exprs)) { }

// PhysicalSort::~PhysicalSort() {}

// //===--------------------------------------------------------------------===//
// // Sink
// //===--------------------------------------------------------------------===//
// // single threaded! need expansion on other modes
// class SortSinkState: public LocalSinkState {
// public:
// 	SortSinkState() {
// 		// TODO content
// 	}

// 	// TODO members
	
// }

// unique_ptr<LocalSinkState> PhysicalSort::GetLocalSinkState() const {
	
// 	auto result = make_unique<SortSinkState>();	// TODO maybe more to initialize!
// 	vector<LogicalType> types;
// 	for (auto &order : order_exprs) {
// 		types.push_back(order.expression->return_type);
// 		result->executor.AddExpression(*order.expression);
// 	}
// 	result->sort.Initialize(types);
// 	return move(result);
// }



// //===--------------------------------------------------------------------===//
// // Source
// //===--------------------------------------------------------------------===//



// //===--------------------------------------------------------------------===//
// // ETC
// //===--------------------------------------------------------------------===//
// std::string PhysicalSort::ParamsToString() const {
// 	return "sort-params";
// }

// std::string PhysicalSort::ToString() const {
// 	return "Sort";
// }

// }