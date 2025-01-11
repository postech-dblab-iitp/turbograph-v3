#pragma once

#include <cassert>
#include <string>
#include "execution/physical_operator/cypher_physical_operator.hpp"
#include "execution/cypher_physical_operator_group.hpp"

namespace duckdb {
	
class CypherPipeline {

public:
	CypherPipeline() {}

	CypherPipeline(CypherPhysicalOperatorGroups& groups, idx_t pipeline_id = 0) : pipeline_id(pipeline_id) {
		operator_groups = groups;
		pipelineLength = groups.size();
	}

	std::vector<CypherPhysicalOperator *> GetOperators() const {
		vector<CypherPhysicalOperator *> result;
		for (size_t i = 0; i < pipelineLength; i++) {
			result.push_back(operator_groups.GetIdxOperator(i));
		}
		return result;
	}

	CypherPhysicalOperator *GetSink() {
		return operator_groups.GetIdxOperator(pipelineLength - 1);
	}

	CypherPhysicalOperator *GetSource() {
		return operator_groups.GetIdxOperator(0);
	}

	CypherPhysicalOperator* GetIdxOperator(int idx) {
		return operator_groups.GetIdxOperator(idx);
	}

	std::string toString() {
		std::string result;
		// sink
		result += "#" + std::to_string(GetSink()->GetOperatorId()) + " " + GetSink()->ToString() + "(" + GetSink()->ParamsToString() + ")\n";
		// operators - reversed
		auto operators = GetOperators();;
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
			result += "#" + std::to_string((*op)->GetOperatorId()) + " " + (*op)->ToString() + "(" + (*op)->ParamsToString() + ")\n";
		}
		// source
		result += "\t|\n";
		if (GetSource()->schemas.size() > 0) {
			result += "    union all schema : " + GetSource()->schema.printStoredColumnAndTypes() + "\n";
			for (auto i = 0; i < GetSource()->schemas.size(); i++) {
				result += "    schema " + std::to_string(i + 1) + " : " + GetSource()->schemas[i].printStoredColumnAndTypes() + "\n";
			}
		} else {
			result += "    " + GetSource()->schema.printStoredColumnAndTypes() + "\n";
		}
		result += "\t|\n";
		result += "#" + std::to_string(GetSource()->GetOperatorId()) + " " + GetSource()->ToString() + "(" + GetSource()->ParamsToString() + ")\n";

		return result;
	}

	idx_t GetPipelineId() {
		return pipeline_id;
	}

	// members
	int pipelineLength;
	//! The unique pipeline id
	idx_t pipeline_id;
	//! The operator groups
	CypherPhysicalOperatorGroups operator_groups;
};

}
