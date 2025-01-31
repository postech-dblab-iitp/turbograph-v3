#pragma once
#include "common/typedef.hpp"

#include "execution/physical_operator/cypher_physical_operator.hpp"
#include "execution/execution_context.hpp"

namespace duckdb {

class PhysicalTop: public CypherPhysicalOperator {

public:
	PhysicalTop(Schema& sch, idx_t limit, idx_t offset)
		: CypherPhysicalOperator(PhysicalOperatorType::TOP, sch), limit(limit), offset(offset)  {
			D_ASSERT(offset == 0 );
	}
	PhysicalTop(Schema& sch, idx_t limit)
		: CypherPhysicalOperator(PhysicalOperatorType::TOP, sch), limit(limit), offset(0) {}

public:
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
	// TODO physical_top can be operator when not incoming as sink. but in multi-threaded execution, it matters.
	OperatorResultType Execute(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &state) const override;

	std::string ParamsToString() const override;
	std::string ToString() const override;

private:
	idx_t offset;
	idx_t limit;
};

}