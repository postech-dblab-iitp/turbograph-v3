#pragma once

// #include "duckdb/common/common.hpp"
// #include "duckdb/catalog/catalog.hpp"
// #include "duckdb/common/enums/physical_operator_type.hpp"

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/enums/operator_result_type.hpp"
// #include "duckdb/common/constants.hpp"

#include "duckdb/execution/physical_operator.hpp"

#include "storage/graph_store.hpp"

#include "typedef.hpp"
#include "storage/graph_store.hpp"

#include "duckdb/common/common.hpp"

using namespace duckdb;

class CypherPhysicalOperator {

public:

	CypherPhysicalOperator( CypherSchema& sch ) : schema(sch), types(schema.getTypes()) {}
	virtual ~CypherPhysicalOperator() { }

	virtual void GetData(GraphStore* graph, DataChunk &chunk, LocalSourceState &lstate) const;
	virtual unique_ptr<LocalSourceState> GetLocalSourceState() const;

	virtual SinkResultType Sink(DataChunk &input, LocalSinkState &lstate) const;
	virtual unique_ptr<LocalSinkState> GetLocalSinkState() const;
	virtual void Combine(LocalSinkState& lstate) const;

	virtual OperatorResultType Execute(DataChunk &input, DataChunk &chunk, OperatorState &state) const;
	virtual unique_ptr<OperatorState> GetOperatorState() const;

	const vector<LogicalType> &GetTypes()  {
		return types;
	}

	virtual std::string ParamsToString() const { return ""; }

private:

	// operator metadata
	CypherSchema schema;
	vector<duckdb::LogicalType> types;
	
};