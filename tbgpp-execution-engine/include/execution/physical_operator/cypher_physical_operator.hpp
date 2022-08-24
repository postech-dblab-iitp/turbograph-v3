#pragma once

// #include "duckdb/common/common.hpp"
// #include "duckdb/catalog/catalog.hpp"
// #include "duckdb/common/enums/physical_operator_type.hpp"

#include "common/types/data_chunk.hpp"
#include "common/enums/operator_result_type.hpp"
// #include "duckdb/common/constants.hpp"

#include "execution/physical_operator.hpp"

#include "typedef.hpp"
#include "execution/execution_context.hpp"


#include "common/common.hpp"

#include <boost/timer/timer.hpp>

namespace duckdb {
struct LogicalType;

class CypherPhysicalOperator {

public:

	CypherPhysicalOperator() {} // sink does not define types
	CypherPhysicalOperator( CypherSchema& sch ) : schema(sch), types(schema.getTypes()) {
		timer_started = false;
		processed_tuples = 0;
	}
	virtual ~CypherPhysicalOperator() { }

	virtual void GetData(ExecutionContext &context, DataChunk &chunk, LocalSourceState &lstate) const;
	virtual unique_ptr<LocalSourceState> GetLocalSourceState() const;

	virtual SinkResultType Sink(ExecutionContext &context, DataChunk &input, LocalSinkState &lstate) const;
	virtual unique_ptr<LocalSinkState> GetLocalSinkState() const;
	virtual void Combine(LocalSinkState& lstate) const;

	virtual OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk, OperatorState &state) const;
	virtual unique_ptr<OperatorState> GetOperatorState() const;

	const vector<LogicalType> &GetTypes();

	virtual std::string ParamsToString() const { return ""; }
	virtual std::string ToString() const { return ""; }

	// operator metadata
	CypherSchema schema;
	vector<LogicalType> types;

	// operator statistics
		// TODO make this into timer struct with some functions
	boost::timer::cpu_timer op_timer;
	bool timer_started;
	int64_t exec_time;

	
	int64_t processed_tuples;
	
};
}