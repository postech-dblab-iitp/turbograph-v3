#pragma once
#include "typedef.hpp"

#include "execution/physical_operator/cypher_physical_operator.hpp"
#include "common/types/value.hpp"

#include "execution/expression_executor.hpp"
#include "planner/expression/bound_conjunction_expression.hpp"

namespace duckdb {

class Filter: public CypherPhysicalOperator {

public:
	Filter(CypherSchema& sch, vector<unique_ptr<Expression>> select_list);
	~Filter() {}

public:

	unique_ptr<OperatorState> GetOperatorState() const override;
	OperatorResultType Execute(GraphStore* graph, DataChunk &input, DataChunk &chunk, OperatorState &state) const override;

	std::string ParamsToString() const override;
	std::string ToString() const override;

	std::unique_ptr<Expression> expression;

};

}