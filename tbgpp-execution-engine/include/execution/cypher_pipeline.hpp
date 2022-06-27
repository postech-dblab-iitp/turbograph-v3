#pragma once

#include <cassert>
// #include <vector>
#include <string>

// #include "duckdb/common/unordered_set.hpp"
// #include "duckdb/execution/physical_operator.hpp"
// #include "duckdb/function/table_function.hpp"
// #include "duckdb/parallel/task_scheduler.hpp"
// #include "duckdb/common/atomic.hpp"

#include "execution/physical_operator/cypher_physical_operator.hpp"

using namespace std;

namespace duckdb {
	
class CypherPipeline {

public:
	CypherPipeline(vector<CypherPhysicalOperator *> ops) {
		assert( ops.size() >= 2 && "too few operators");

		source = ops.front();
		sink = ops.back();
		pipelineLength = ops.size();

		ops.erase(ops.begin());
		ops.pop_back();
		operators = ops;

	}

	std::vector<CypherPhysicalOperator *> GetOperators() {
		return operators;	
	}

	CypherPhysicalOperator *GetSink() {
		return sink;
	}

	CypherPhysicalOperator *GetSource() {
		return source;
	}

	CypherPhysicalOperator* GetIdxOperator(int idx) {
		if( idx == 0 ) {
			return source;
		}
		if ( idx == pipelineLength - 1) {
			return sink;
		}
		// operator
		return operators[idx - 1];
	}

	std::string toString() {
		
		std::string result;
		// sink
		result += sink->ToString() + "(" + sink->ParamsToString() + ")\n";
		// operators - reversed
		for (auto op = operators.rbegin(); op != operators.rend(); ++op) {
			result += "\t|\n";
			result += "    " + (*op)->schema.toString() + "\n";
			result += "\t|\n";
			result += (*op)->ToString() + "(" + (*op)->ParamsToString() + ")\n";
		}
		// source
		result += "\t|\n";
		result += "    " + source->schema.toString() + "\n";
		result += "\t|\n";
		result += source->ToString() + "(" + source->ParamsToString() + ")\n";

		return result;
	}

	// members
	int pipelineLength;

	//! The source of this pipeline
	CypherPhysicalOperator *source;
	//! The chain of intermediate operators
	std::vector<CypherPhysicalOperator *> operators;	// TODO name is strange!!!!! need to fix.
	//! The sink (i.e. destination) for data; this is e.g. a hash table to-be-built
	CypherPhysicalOperator *sink;

	// TODO need to record parents
	// when the pipelines have hierarchy.

};

}
