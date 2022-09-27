#pragma once
#include "typedef.hpp"

#include "execution/physical_operator/cypher_physical_operator.hpp"
#include "common/enums/join_type.hpp"
#include "planner/joinside.hpp"

#include <boost/timer/timer.hpp>
#include <unordered_set>

#include <cassert>

namespace duckdb {

class PhysicalAdjIdxJoin: public CypherPhysicalOperator {

public:
	PhysicalAdjIdxJoin(CypherSchema& sch,
		std::string srcName, LabelSet srcLabelSet,				// source
		LabelSet edgeLabelSet, ExpandDirection expandDir,		// edge			// serves as first join condtion (lhs.vid = rhs.vid)
		LabelSet tgtLabelSet,									// target		// serves as second join condition (label(rhs.vid) == XX)
		JoinType join_type,										// join type
		bool load_eid, bool enumerate);							// details

	PhysicalAdjIdxJoin(CypherSchema& sch,
		std::string srcName, LabelSet srcLabelSet,				// source
		LabelSet edgeLabelSet, ExpandDirection expandDir,		// edge
		LabelSet tgtLabelSet,									// target
		JoinType join_type,										// join type
		vector<JoinCondition> remaining_conditions,				// join conditions // remaining join conditions  
		bool load_eid, bool enumerate);							// details

	~PhysicalAdjIdxJoin() {}

public:
	
	// common interface
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
	OperatorResultType Execute(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &state) const override;
	// locally used functions
	OperatorResultType ExecuteRangedInput(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const;
	OperatorResultType ExecuteNaiveInput(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const;
	void GetJoinMatches(ExecutionContext& context, DataChunk &input, OperatorState &lstate) const;
	void ProcessSemiAntiJoin(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const;
	void ProcessEquiJoin(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const;
	void ProcessLeftJoin(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &lstate) const;

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
	// join type
	JoinType join_type;
	// remaining join conditions
	vector<JoinCondition> remaining_conditions;
	bool load_eid;
	bool enumerate;

	// performance counter
	// mutable boost::timer::cpu_timer adjfetch_timer;
	// mutable bool adjfetch_timer_started;

	mutable boost::timer::cpu_timer timer2;
	mutable bool timer2_started;

	mutable boost::timer::cpu_timer timer1;
	mutable bool timer1_started;

};

}