#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "storage/graph_store.hpp"

#include "typedef.hpp"
#include "storage/graph_store.hpp"

using namespace duckdb;

class CypherPhysicalOperator: public duckdb::PhysicalOperator {

public:
	// TODO caredest disabled. 
	// TODO physical operator type disabled. do it later.

	CypherPhysicalOperator( CypherSchema schema ) :
		duckdb::PhysicalOperator(duckdb::PhysicalOperatorType::INVALID, schema.getTypes(), 0), schema(schema){};

	virtual void GetData(GraphStore* graph, DataChunk &chunk, LocalSourceState &lstate) const;
	virtual unique_ptr<LocalSourceState> GetLocalSourceState() const;

	virtual SinkResultType Sink(DataChunk &input, LocalSinkState &lstate) const;
	virtual unique_ptr<LocalSinkState> GetLocalSinkState() const;

	virtual OperatorResultType Execute(DataChunk &input, DataChunk &chunk, OperatorState &state) const;
	virtual unique_ptr<OperatorState> GetOperatorState() const;

	// TODO execute

private:
	// TODO additional members

	// operator metadata
	CypherSchema schema;
	
};