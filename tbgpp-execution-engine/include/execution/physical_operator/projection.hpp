// #pragma once
// #include "typedef.hpp"

// #include "execution/physical_operator/cypher_physical_operator.hpp"
// #include "duckdb/planner/expression.hpp"


// #include <vector>

// namespace duckdb {

// class Projection: public CypherPhysicalOperator {

// public:
// 	Projection(CypherSchema& sch, std::vector<unique_ptr<Expression>> select_list)
// 		: CypherPhysicalOperator(sch), select_list(move(select_list)) { }
// 	~Projection() {}

// public:

// 	unique_ptr<OperatorState> GetOperatorState() const override;
// 	OperatorResultType Execute(GraphStore* graph, DataChunk &input, DataChunk &chunk, OperatorState &state) const override;

// 	std::string ParamsToString() const override;
// 	std::string ToString() const override;
	
// 	vector<unique_ptr<Expression>> select_list;

// };

// }