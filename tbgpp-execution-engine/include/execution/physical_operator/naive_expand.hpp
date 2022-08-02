#pragma once
#include "typedef.hpp"

#include "execution/physical_operator/cypher_physical_operator.hpp"
#include "storage/graph_store.hpp"

#include <boost/timer/timer.hpp>

namespace duckdb {

class NaiveExpand: public CypherPhysicalOperator {

public:
	NaiveExpand(CypherSchema& sch,
		std::string srcName, LabelSet srcLabelSet, LabelSet srcEdgeLabelSet, ExpandDirection expandDir, std::string edgeName, LabelSet tgtLabelSet, std::vector<LabelSet> tgtEdgeLabelSets, LoadAdjListOption tgtLoadAdjOpt, PropertyKeys tgtPropertyKeys)
		: CypherPhysicalOperator(sch),
		srcName(srcName), srcLabelSet(srcLabelSet), srcEdgeLabelSet(srcEdgeLabelSet), expandDir(expandDir), edgeName(edgeName), tgtLabelSet(tgtLabelSet), tgtEdgeLabelSets(tgtEdgeLabelSets), tgtLoadAdjOpt(tgtLoadAdjOpt), tgtPropertyKeys(tgtPropertyKeys)
	{
		// init timers
		adjfetch_time = 0;
		tgtfetch_time = 0;	
		assert( expandDir == ExpandDirection::OUTGOING && "currently supports outgoing index"); // TODO needs support from the storage
	}
	~NaiveExpand() {}

public:
	
	unique_ptr<OperatorState> GetOperatorState() const override;
	OperatorResultType Execute(GraphStore* graph, DataChunk &input, DataChunk &chunk, OperatorState &state) const override;

	std::string ParamsToString() const override;
	std::string ToString() const override;

	// operator parameters
		// src
	LabelSet srcLabelSet; // TODO wrong this should not be kept
	std::string srcName;
	LabelSet srcEdgeLabelSet;	// not meaningful currently
	ExpandDirection expandDir;
		// edge
	std::string edgeName;
		// tgt
	LabelSet tgtLabelSet;
	std::vector<LabelSet> tgtEdgeLabelSets;
	LoadAdjListOption tgtLoadAdjOpt;
	PropertyKeys tgtPropertyKeys;

	// performance counter
	boost::timer::cpu_timer adjfetch_timer;
	bool adjfetch_timer_started;
	int64_t adjfetch_time;

	boost::timer::cpu_timer tgtfetch_timer;
	bool tgtfetch_timer_started;
	int64_t tgtfetch_time;

};

}