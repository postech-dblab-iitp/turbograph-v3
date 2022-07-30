// #pragma once
// #include "typedef.hpp"

// #include "execution/physical_operator/cypher_physical_operator.hpp"

// #include <vector>

// namespace duckdb {

// class Limit: public CypherPhysicalOperator {

// public:
// 	Limit(CypherSchema& sch, uint64_t count)
// 		: CypherPhysicalOperator(sch), count(count) { }
// 	~Limit() {}

// public:

// 	unique_ptr<OperatorState> GetOperatorState() const override;
// 	OperatorResultType Execute(GraphStore* graph, DataChunk &input, DataChunk &chunk, OperatorState &state) const override;

// 	std::string ParamsToString() const override;
// 	std::string ToString() const override;
	
// 	uint64_t count;

// };

// }