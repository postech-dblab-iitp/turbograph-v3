#pragma once

#include <cassert>
#include <string>
#include "execution/physical_operator/cypher_physical_operator.hpp"

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
	std::vector<CypherPhysicalOperator *> &GetOperators() {
		return operators;	
	}

	std::vector<CypherPhysicalOperator *> GetOperators() const {
		vector<CypherPhysicalOperator *> result;
		D_ASSERT(source);
		result.push_back(source);
		for (auto &op : operators) {
			result.push_back(op);
		}
		if (sink) {
			result.push_back(sink);
		}
		return result;
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
			if ((*op)->schemas.size() > 0) {
				for (auto i = 0; i < (*op)->schemas.size(); i++) {
					result += "    schema " + std::to_string(i + 1) + ": " + (*op)->schemas[i].printStoredColumnAndTypes() + "\n";
				}
			} else {
				result += "    " + (*op)->schema.printStoredColumnAndTypes() + "\n";
			}
			result += "\t|\n";
			result += (*op)->ToString() + "(" + (*op)->ParamsToString() + ")\n";
		}
		// source
		result += "\t|\n";
		if (source->schemas.size() > 0) {
			result += "    union all schema : " + source->schema.printStoredColumnAndTypes() + "\n";
			for (auto i = 0; i < source->schemas.size(); i++) {
				result += "    schema " + std::to_string(i + 1) + " : " + source->schemas[i].printStoredColumnAndTypes() + "\n";
			}
		} else {
			result += "    " + source->schema.printStoredColumnAndTypes() + "\n";
		}
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

};

}
