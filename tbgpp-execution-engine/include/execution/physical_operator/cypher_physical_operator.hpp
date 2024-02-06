#pragma once

// #include "common/common.hpp"
// #include "catalog/catalog.hpp"
#include "common/common.hpp"
#include "common/enums/operator_result_type.hpp"
#include "common/enums/physical_operator_type.hpp"
#include "common/types/data_chunk.hpp"
#include "execution/execution_context.hpp"
#include "execution/physical_operator/physical_operator.hpp"
#include "typedef.hpp"

#include <boost/timer/timer.hpp>

namespace duckdb {
struct LogicalType;

class CypherPhysicalOperator {

   public:
    CypherPhysicalOperator(PhysicalOperatorType type, Schema &schema)
        : type(type),
          schema(schema),
          types(schema.getStoredTypes()),
          processed_tuples(0)
    {
        timer_started = false;
    }
    CypherPhysicalOperator(PhysicalOperatorType type, vector<Schema> &schemas)
        : type(type),
          schemas(schemas),
          types(schemas[0].getStoredTypes()),
          processed_tuples(0)
    {  // TODO
        timer_started = false;
    }
    CypherPhysicalOperator(PhysicalOperatorType type, Schema &union_schema,
                           vector<Schema> &schemas)
        : type(type),
          schema(union_schema),
          schemas(schemas),
          types(union_schema.getStoredTypes()),
          processed_tuples(0)
    {  // TODO
        timer_started = false;
    }
    virtual ~CypherPhysicalOperator() {}

    // leaf sources (e.g. Scan)
    virtual void GetData(ExecutionContext &context, DataChunk &chunk,
                         LocalSourceState &lstate) const;
    virtual bool IsSourceDataRemaining(LocalSourceState &lstate) const;
    // non-leaf sources (e.g. HashAgg, Sort source)
    virtual void GetData(ExecutionContext &context, DataChunk &chunk,
                         LocalSourceState &lstate,
                         LocalSinkState &sink_state) const;
    virtual bool IsSourceDataRemaining(LocalSourceState &lstate,
                                       LocalSinkState &sink_state) const;
    virtual unique_ptr<LocalSourceState> GetLocalSourceState(
        ExecutionContext &context) const;
    virtual bool IsSource() const
    {
        return false;
    }  // must be overrided for true for source operators

    virtual SinkResultType Sink(ExecutionContext &context, DataChunk &input,
                                LocalSinkState &lstate) const;
    virtual unique_ptr<LocalSinkState> GetLocalSinkState(
        ExecutionContext &context) const;
    virtual void Combine(ExecutionContext &context,
                         LocalSinkState &lstate) const;
    virtual bool IsSink() const
    {
        return false;
    }  // must be overrided for true for sink operators

    // standalone piped operators (e.g. Scan)
    virtual OperatorResultType Execute(ExecutionContext &context,
                                       DataChunk &input, DataChunk &chunk,
                                       OperatorState &state) const;
    virtual OperatorResultType Execute(ExecutionContext &context,
                                       DataChunk &input,
                                       vector<unique_ptr<DataChunk>> &chunks,
                                       OperatorState &state,
                                       idx_t &output_chunk_idx) const;
    // sink-enabled piped operators (e.g. Hash Probe, CartProd)
    virtual OperatorResultType Execute(ExecutionContext &context,
                                       DataChunk &input, DataChunk &chunk,
                                       OperatorState &state,
                                       LocalSinkState &sink_state) const;
    virtual OperatorResultType Execute(ExecutionContext &context,
                                       DataChunk &input,
                                       vector<unique_ptr<DataChunk>> &chunks,
                                       OperatorState &state,
                                       LocalSinkState &sink_state,
                                       idx_t &output_chunk_idx) const;
    virtual unique_ptr<OperatorState> GetOperatorState(
        ExecutionContext &context) const;
    virtual DataChunk &GetLastSinkedData(LocalSinkState &lstate) const;

    const vector<LogicalType> &GetTypes();
    idx_t GetSchemaIdx();

    virtual std::string ParamsToString() const { return ""; }
    virtual std::string ToString() const { return ""; }

    // operator metadata
    const PhysicalOperatorType type;
    mutable Schema schema;           // TODO remove mutable
    mutable vector<Schema> schemas;  // TODO remove mutable
    mutable vector<LogicalType>
        types;  // schema(types) of operator output chunk
    vector<CypherPhysicalOperator *> children;  // child operators

    // operator statistics
    // TODO make this into timer struct with some functions
    boost::timer::cpu_timer op_timer;  // TODO deprecate
    bool timer_started;
    int64_t exec_time;

    int64_t processed_tuples;
};
}  // namespace duckdb