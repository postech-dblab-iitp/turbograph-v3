#pragma once
#include "typedef.hpp"

#include "execution/physical_operator/cypher_physical_operator.hpp"
#include "common/enums/join_type.hpp"

#include <boost/timer/timer.hpp>
#include <unordered_set>

#include <cassert>

namespace duckdb {

class PhysicalAdjIdxJoin: public CypherPhysicalOperator {

public:
	PhysicalAdjIdxJoin(CypherSchema& sch,
		std::string srcName, LabelSet srcLabelSet, LabelSet edgeLabelSet, ExpandDirection expandDir, LabelSet tgtLabelSet, JoinType join_type, bool load_eid, bool enumerate);
	// TODO add interface with predicates : remaining predicates should be applied - look JoinCondition

	~PhysicalAdjIdxJoin() {}

public:
	
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
	OperatorResultType Execute(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &state) const override;
	OperatorResultType ExecuteNaiveInput(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &state) const;
	OperatorResultType ExecuteRangedInput(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &state) const;

	std::string ParamsToString() const override;
	std::string ToString() const override;

	// operator parameters

	std::string srcName;
	// this is not actually necessary... src should be already given as node list. should further be removed.
	LabelSet srcLabelSet;

	// list of edges to scan (empty means all possible edges, otherwise, union items).
	// Equivalent as performing join with multiple tables : e.g. R join S U R join S' -> R join (S U S')
	LabelSet edgeLabelSet;
	ExpandDirection expandDir;

	// join condition (filter) on target labels (empty means all existing nodes, intersection on multiple labels)
	LabelSet tgtLabelSet;
	// remaining join conditions
	// TODO add attribute here

	JoinType join_type;

	bool load_eid;
	bool enumerate;

	// performance counter
	mutable boost::timer::cpu_timer adjfetch_timer;
	mutable bool adjfetch_timer_started;

};

}