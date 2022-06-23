#pragma once

#include <cassert>
#include <vector>

#include "duckdb/common/unordered_set.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/common/atomic.hpp"

#include "execution/physical_operator/cypher_physical_operator.hpp"

using namespace std;

class CypherPipeline {

public:
	CypherPipeline(vector<CypherPhysicalOperator *> ops) {
		assert( ops.size() > 2 && "too few operators");

		source = ops.front();
		sink = ops.back();

		ops.erase(ops.begin());
		ops.erase(ops.end());
		operators = ops;

		pipelineLength = ops.size();
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