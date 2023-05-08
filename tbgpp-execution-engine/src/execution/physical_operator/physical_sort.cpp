// #include "execution/physical_operator/physical_sort.hpp"

// #include "planner/expression.hpp"
// #include "planner/bound_result_modifier.hpp"
// #include "common/sort/sort.hpp"
// #include "execution/expression_executor.hpp"
// #include "main/client_context.hpp"
// #include "storage/buffer_manager.hpp"

// namespace duckdb {

// PhysicalSort::PhysicalSort(Schema& sch, vector<BoundOrderByNode> orders_p):
// 	CypherPhysicalOperator(sch), orders(std::move(orders_p)) { }

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
// 	//! Global sort state
// 	GlobalSortState global_sort_state;
// 	//! Memory usage per thread
// 	idx_t memory_per_thread;
// 	//! The local sort state
// 	LocalSortState local_sort_state;
// 	//! Local copy of the sorting expression executor
// 	ExpressionExecutor executor;
// 	//! Holds a vector of incoming sorting columns
// 	DataChunk sort;
	
// }

// unique_ptr<LocalSinkState> PhysicalSort::GetLocalSinkState() const {
	
// 	auto result = make_unique<SortSinkState>();	// TODO maybe more to initialize!
// 	vector<LogicalType> types;
// 	for (auto &order : orders) {
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