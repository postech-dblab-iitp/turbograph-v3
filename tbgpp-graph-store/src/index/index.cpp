#include "index/index.hpp"
// #include "execution/expression_executor.hpp"
// #include "planner/expression_iterator.hpp"
// #include "planner/expression/bound_columnref_expression.hpp"
// #include "planner/expression/bound_reference_expression.hpp"
// #include "storage/table/append_state.hpp"

namespace duckdb {

Index::Index(IndexType type, const vector<column_t> &column_ids_p, IndexConstraintType constraint_type_p)
    : type(type), column_ids(column_ids_p), constraint_type(constraint_type_p) {
	// temporary
	if (column_ids.size() == 1) {
		types.push_back(PhysicalType::UINT64);
		logical_types.push_back(LogicalType::UBIGINT);
	} else if (column_ids.size() == 2) {
		types.push_back(PhysicalType::INT128);
		logical_types.push_back(LogicalType::HUGEINT);
	} else throw InvalidInputException("");
	// for (auto &expr : unbound_expressions) {
	// 	types.push_back(expr->return_type.InternalType());
	// 	logical_types.push_back(expr->return_type);
	// 	auto unbound_expression = expr->Copy();
	// 	bound_expressions.push_back(BindExpression(unbound_expression->Copy()));
	// 	this->unbound_expressions.emplace_back(move(unbound_expression));
	// }
	// for (auto &bound_expr : bound_expressions) {
	// 	executor.AddExpression(*bound_expr);
	// }
	for (auto column_id : column_ids) {
		column_id_set.insert(column_id);
	}
}

vector<column_t> Index::GetColumnIds() {
	vector<column_t> return_column_ids;
	for (size_t i = 0; i < column_ids.size(); i++) return_column_ids.push_back(column_ids[i]);
	return return_column_ids;
}

// void Index::InitializeLock(IndexLock &state) {
// 	state.index_lock = unique_lock<mutex>(lock);
// }

// bool Index::Append(DataChunk &entries, Vector &row_identifiers) {
// 	IndexLock state;
// 	InitializeLock(state);
// 	return Append(state, entries, row_identifiers);
// }

// void Index::Delete(DataChunk &entries, Vector &row_identifiers) {
// 	IndexLock state;
// 	InitializeLock(state);
// 	Delete(state, entries, row_identifiers);
// }

// void Index::ExecuteExpressions(DataChunk &input, DataChunk &result) {
// 	executor.Execute(input, result);
// }

// unique_ptr<Expression> Index::BindExpression(unique_ptr<Expression> expr) {
// 	if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
// 		auto &bound_colref = (BoundColumnRefExpression &)*expr;
// 		return make_unique<BoundReferenceExpression>(expr->return_type, column_ids[bound_colref.binding.column_index]);
// 	}
// 	ExpressionIterator::EnumerateChildren(*expr,
// 	                                      [&](unique_ptr<Expression> &expr) { expr = BindExpression(move(expr)); });
// 	return expr;
// }

// bool Index::IndexIsUpdated(const vector<column_t> &column_ids) const {
// 	for (auto &column : column_ids) {
// 		if (column_id_set.find(column) != column_id_set.end()) {
// 			return true;
// 		}
// 	}
// 	return false;
// }

} // namespace duckdb
