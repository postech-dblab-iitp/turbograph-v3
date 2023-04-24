#pragma once
#include "typedef.hpp"

#include "execution/physical_operator/cypher_physical_operator.hpp"
#include "execution/execution_context.hpp"
#include "planner/expression.hpp"
#include "planner/expression/bound_conjunction_expression.hpp"


#include "common/types/value.hpp"

namespace duckdb {

class PhysicalFilter: public CypherPhysicalOperator {

public:
	PhysicalFilter(CypherSchema& sch, vector<unique_ptr<Expression>> predicates)
		: CypherPhysicalOperator(PhysicalOperatorType::FILTER, sch) {
			
		D_ASSERT(predicates.size() > 0);
		if (predicates.size() > 1) {
			auto conjunction = make_unique<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
			for (auto &expr : predicates) {
				conjunction->children.push_back(move(expr));
			}
			expression = move(conjunction);
		} else {
			expression = move(predicates[0]);
		}
	}
	~PhysicalFilter() {}

public:

	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
	OperatorResultType Execute(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &state) const override;

	std::string ParamsToString() const override;
	std::string ToString() const override;

	unique_ptr<Expression> expression;
};

}