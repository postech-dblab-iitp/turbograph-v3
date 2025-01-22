// #include "planner/expression/bound_subquery_expression.hpp"

// #include "common/exception.hpp"

// namespace s62 {

// BoundSubqueryExpression::BoundSubqueryExpression(LogicalType return_type)
//     : Expression(ExpressionType::SUBQUERY, ExpressionClass::BOUND_SUBQUERY, move(return_type)) {
// }

// string BoundSubqueryExpression::ToString() const {
// 	return "SUBQUERY";
// }

// bool BoundSubqueryExpression::Equals(const BaseExpression *other_p) const {
// 	// equality between bound subqueries not implemented currently
// 	return false;
// }

// unique_ptr<Expression> BoundSubqueryExpression::Copy() {
// 	throw SerializationException("Cannot copy BoundSubqueryExpression");
// }

// bool BoundSubqueryExpression::PropagatesNullValues() const {
// 	
// 	return false;
// }

// } // namespace s62
