#pragma once
#include "typedef.hpp"

#include "execution/physical_operator/cypher_physical_operator.hpp"
#include "execution/execution_context.hpp"
#include "planner/expression.hpp"
#include "planner/expression/bound_conjunction_expression.hpp"

#include "common/types/value.hpp"

namespace duckdb {

class PhysicalProjection: public CypherPhysicalOperator {

public:
	PhysicalProjection(CypherSchema& sch, vector<unique_ptr<Expression>> expressions)
		: CypherPhysicalOperator(sch), expressions(move(expressions)) { }
	~PhysicalProjection() {}

public:

	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
	OperatorResultType Execute(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &state) const override;

	std::string ParamsToString() const override;
	std::string ToString() const override;

	vector<unique_ptr<Expression>> expressions;	// projection expression per each column
};

}