// //===----------------------------------------------------------------------===//
// //                         DuckDB
// //
// // duckdb/planner/logical_operator.hpp
// //
// //
// //===----------------------------------------------------------------------===//

// #pragma once

// //#include "catalog/catalog.hpp"
// #include "common/common.hpp"
// #include "common/enums/logical_operator_type.hpp"
// #include "planner/expression.hpp"
// #include "planner/logical_operator_visitor.hpp"
// #include "planner/column_binding.hpp"

// #include <functional>
// #include <algorithm>

// namespace duckdb {

// //! LogicalOperator is the base class of the logical operators present in the
// //! logical query tree
// class LogicalOperator {
// public:
// 	explicit LogicalOperator(LogicalOperatorType type);
// 	LogicalOperator(LogicalOperatorType type, vector<unique_ptr<Expression>> expressions);
// 	virtual ~LogicalOperator();

// 	//! The type of the logical operator
// 	LogicalOperatorType type;
// 	//! The set of children of the operator
// 	vector<unique_ptr<LogicalOperator>> children;
// 	//! The set of expressions contained within the operator, if any
// 	vector<unique_ptr<Expression>> expressions;
// 	//! The types returned by this logical operator. Set by calling LogicalOperator::ResolveTypes.
// 	vector<LogicalType> types;
// 	//! Estimated Cardinality
// 	idx_t estimated_cardinality = 0;

// public:
// 	virtual vector<ColumnBinding> GetColumnBindings();
// 	static vector<ColumnBinding> GenerateColumnBindings(idx_t table_idx, idx_t column_count);
// 	static vector<LogicalType> MapTypes(const vector<LogicalType> &types, const vector<idx_t> &projection_map);
// 	static vector<ColumnBinding> MapBindings(const vector<ColumnBinding> &types, const vector<idx_t> &projection_map);

// 	//! Resolve the types of the logical operator and its children
// 	void ResolveOperatorTypes();

// 	virtual string GetName() const;
// 	virtual string ParamsToString() const;
// 	virtual string ToString() const;
// 	DUCKDB_API void Print();
// 	//! Debug method: verify that the integrity of expressions & child nodes are maintained
// 	virtual void Verify();

// 	void AddChild(unique_ptr<LogicalOperator> child);

// 	//virtual idx_t EstimateCardinality(ClientContext &context);

// protected:
// 	//! Resolve types for this specific operator
// 	virtual void ResolveTypes() = 0;
// };
// } // namespace duckdb
