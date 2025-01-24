#include "execution/physical_operator/physical_join.hpp"

namespace duckdb {

PhysicalJoin::PhysicalJoin(Schema& sch, PhysicalOperatorType type, JoinType join_type)
    : CypherPhysicalOperator(type, sch), join_type(join_type) {
}

bool PhysicalJoin::EmptyResultIfRHSIsEmpty() const {
	// empty RHS with INNER, RIGHT or SEMI join means empty result set
	switch (join_type) {
	case JoinType::INNER:
	case JoinType::RIGHT:
	case JoinType::SEMI:
		return true;
	default:
		return false;
	}
}

} // namespace duckdb
