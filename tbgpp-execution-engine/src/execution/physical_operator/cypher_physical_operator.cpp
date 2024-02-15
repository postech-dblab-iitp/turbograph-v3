#include "execution/physical_operator/cypher_physical_operator.hpp"
#include "common/enums/operator_result_type.hpp"
#include "common/exception.hpp"
#include "common/types/schemaless_data_chunk.hpp"
#include "execution/execution_context.hpp"

namespace duckdb {

void CypherPhysicalOperator::GetData(ExecutionContext &context,
                                     DataChunk &chunk,
                                     LocalSourceState &lstate) const
{
    throw InternalException("Calling GetData on a node that is not a source!");
}
void CypherPhysicalOperator::GetData(ExecutionContext &context,
                                     DataChunk &chunk, LocalSourceState &lstate,
                                     LocalSinkState &sink_state) const
{
    throw InternalException("Calling GetData on a node that is not a source!");
}

unique_ptr<LocalSourceState> CypherPhysicalOperator::GetLocalSourceState(
    ExecutionContext &context) const
{
    return make_unique<LocalSourceState>();
}

SinkResultType CypherPhysicalOperator::Sink(ExecutionContext &context,
                                            DataChunk &input,
                                            LocalSinkState &lstate) const
{
    throw InternalException("Calling Sink on a node that is not a sink!");
}
unique_ptr<LocalSinkState> CypherPhysicalOperator::GetLocalSinkState(
    ExecutionContext &context) const
{
    return make_unique<LocalSinkState>();
}

bool CypherPhysicalOperator::IsSourceDataRemaining(
    LocalSourceState &lstate) const
{
    throw InternalException(
        "Calling IsSourceDataRemaining on a node that is not a source!");
}

bool CypherPhysicalOperator::IsSourceDataRemaining(
    LocalSourceState &lstate, LocalSinkState &sink_state) const
{
    throw InternalException(
        "Calling IsSourceDataRemaining on a node that is not a source!");
}

void CypherPhysicalOperator::Combine(ExecutionContext &context,
                                     LocalSinkState &lstate) const
{
    // nothing
}

OperatorResultType CypherPhysicalOperator::Execute(ExecutionContext &context,
                                                   DataChunk &input,
                                                   DataChunk &chunk,
                                                   OperatorState &state) const
{
    throw InternalException(
        "Calling Execute on a node that is not an operator!");
}

OperatorResultType CypherPhysicalOperator::Execute(
    ExecutionContext &context, DataChunk &input,
    vector<unique_ptr<DataChunk>> &chunks, OperatorState &state,
    idx_t &output_chunk_idx) const
{
    throw InternalException(
        "Calling Execute on a node that is not an operator!");
}

OperatorResultType CypherPhysicalOperator::Execute(
    ExecutionContext &context, DataChunk &input, DataChunk &chunk,
    OperatorState &state, LocalSinkState &sink_state) const
{
    throw InternalException(
        "Calling Execute on a node that is not an operator!");
}

unique_ptr<OperatorState> CypherPhysicalOperator::GetOperatorState(
    ExecutionContext &context) const
{
    return make_unique<OperatorState>();
}

DataChunk &CypherPhysicalOperator::GetLastSinkedData(
    LocalSinkState &lstate) const
{
    throw InternalException(
        "Calling GetLastSinkedData on a node that is not a sink!");
}

void CypherPhysicalOperator::InitializeOutputChunk(DataChunk &output_chunk)
{
    // nothing
}

void CypherPhysicalOperator::InitializeOutputChunks(
    std::vector<unique_ptr<DataChunk>> &output_chunks, Schema &output_schema,
    idx_t idx)
{
    // auto opOutputChunk = std::make_unique<SchemalessDataChunk>();
    auto opOutputChunk = std::make_unique<DataChunk>();
    opOutputChunk->Initialize(output_schema.getStoredTypes());
    output_chunks.push_back(std::move(opOutputChunk));
}

const vector<LogicalType> &CypherPhysicalOperator::GetTypes()
{
    return types;
}

}  // namespace duckdb