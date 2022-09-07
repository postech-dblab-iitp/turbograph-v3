#include "execution/physical_operator/cypher_physical_operator.hpp"
#include "execution/execution_context.hpp"

#include "common/exception.hpp"

#include "common/enums/operator_result_type.hpp"

namespace duckdb {

void CypherPhysicalOperator::GetData(ExecutionContext& context, DataChunk &chunk, LocalSourceState &lstate) const {
	throw InternalException("Calling GetData on a node that is not a source!");
}
void CypherPhysicalOperator::GetData(ExecutionContext& context, DataChunk &chunk, LocalSourceState &lstate, LocalSinkState& sink_state) const {
	throw InternalException("Calling GetData on a node that is not a source!");
}

unique_ptr<LocalSourceState> CypherPhysicalOperator::GetLocalSourceState(ExecutionContext &context) const{
	return make_unique<LocalSourceState>();
}

SinkResultType CypherPhysicalOperator::Sink(ExecutionContext& context, DataChunk &input, LocalSinkState &lstate) const {
	throw InternalException("Calling Sink on a node that is not a sink!");
}
unique_ptr<LocalSinkState> CypherPhysicalOperator::GetLocalSinkState(ExecutionContext &context) const{
	return make_unique<LocalSinkState>();
}
void CypherPhysicalOperator::Combine(ExecutionContext& context, LocalSinkState& lstate) const {
	// nothing
}

OperatorResultType CypherPhysicalOperator::Execute(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &state) const {
	throw InternalException("Calling Execute on a node that is not an operator!");
}
unique_ptr<OperatorState> CypherPhysicalOperator::GetOperatorState(ExecutionContext &context) const{
	return make_unique<OperatorState>();
}

const vector<LogicalType>& CypherPhysicalOperator::GetTypes()  {
	return types;
}

}