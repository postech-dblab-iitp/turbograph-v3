#pragma once

#include "typedef.hpp"
#include "execution/physical_operator/cypher_physical_operator.hpp"
#include "main/client_context.hpp"

namespace duckdb {

class PhysicalUnwind: public CypherPhysicalOperator {
public:
	PhysicalUnwind(CypherSchema& sch, idx_t col_idx);
	~PhysicalUnwind();

	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
	OperatorResultType Execute(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &state) const override;

	std::string ParamsToString() const override;
	std::string ToString() const override;

	// parameter
	idx_t col_idx;

};

}