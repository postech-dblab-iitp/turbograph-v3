#pragma once

// #include "common/common.hpp"
// #include "catalog/catalog.hpp"
// #include "common/enums/physical_operator_type.hpp"

#include "common/types/data_chunk.hpp"
#include "common/enums/operator_result_type.hpp"
// #include "common/constants.hpp"

#include "execution/physical_operator/physical_operator.hpp"

#include "typedef.hpp"
#include "execution/execution_context.hpp"


#include "common/common.hpp"

#include <boost/timer/timer.hpp>

namespace duckdb {
struct LogicalType;

class CypherPhysicalOperator {

public:

	CypherPhysicalOperator() {} // sink does not define types

	// TODO S62 further need to not store cypherschema
	CypherPhysicalOperator( CypherSchema& sch ) : schema(sch), types(schema.getStoredTypes()) {	
		timer_started = false;
		processed_tuples = 0;
	}
	virtual ~CypherPhysicalOperator() { }

	// leaf sources (e.g. Scan)
	virtual void GetData(ExecutionContext &context, DataChunk &chunk, LocalSourceState &lstate) const;
	// non-leaf sources (e.g. HashAgg, Sort source)
	virtual void GetData(ExecutionContext &context, DataChunk &chunk, LocalSourceState &lstate, LocalSinkState &sink_state) const;
	virtual unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context) const;
	virtual bool IsSource() const { return false; }	// must be overrided for true for source operators

	virtual SinkResultType Sink(ExecutionContext &context, DataChunk &input, LocalSinkState &lstate) const;
	virtual unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const;
	virtual void Combine(ExecutionContext& context, LocalSinkState& lstate) const;
	virtual bool IsSink() const { return false; }	// must be overrided for true for sink operators

	// standalone piped operators (e.g. Scan)
	virtual OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk, OperatorState &state) const;
	// sink-enabled piped operators (e.g. Hash Probe, CartProd)
	virtual OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk, OperatorState &state, LocalSinkState &sink_state) const;
	virtual unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const;

	const vector<LogicalType> &GetTypes();

	virtual std::string ParamsToString() const { return ""; }
	virtual std::string ToString() const { return ""; }

	// operator metadata
	// TODO remove mutable
	mutable CypherSchema schema;
	mutable vector<LogicalType> types;

	// operator statistics
		// TODO make this into timer struct with some functions
	boost::timer::cpu_timer op_timer;
	bool timer_started;
	int64_t exec_time;
	
	int64_t processed_tuples;
	
};
}