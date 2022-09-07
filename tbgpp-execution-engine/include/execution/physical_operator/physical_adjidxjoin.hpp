#pragma once
#include "typedef.hpp"

#include "execution/physical_operator/cypher_physical_operator.hpp"
#include "common/enums/join_type.hpp"

#include <boost/timer/timer.hpp>

#include <cassert>

namespace duckdb {

class PhysicalAdjIdxJoin: public CypherPhysicalOperator {

public:
	PhysicalAdjIdxJoin(CypherSchema& sch,
		std::string srcName, LabelSet srcLabelSet, LabelSet edgeLabelSet, ExpandDirection expandDir, LabelSet tgtLabelSet, JoinType join_type, bool load_eid, bool enumerate)
		: CypherPhysicalOperator(sch), srcName(srcName), srcLabelSet(srcLabelSet), edgeLabelSet(edgeLabelSet), expandDir(expandDir), tgtLabelSet(tgtLabelSet), join_type(join_type), load_eid(load_eid), enumerate(enumerate) {
		
		// init timers
		adjfetch_time = 0;
		tgtfetch_time = 0;

		// operator rules
		bool check = (enumerate) ? true : (!load_eid);
		assert( check && "load_eid should be set to false(=not returning edge ids) when `enumerate` set to `false` (=range)");

		// TODO assert
		assert( expandDir == ExpandDirection::OUTGOING && "currently supports outgoing index. how to do for both direction or incoming?"); // TODO needs support from the storage
		assert( tgtLabelSet.size() == 0 && "currently do not filter target labels, Storage API support needed"); // TODO needs support from the storage
		assert( edgeLabelSet.size() == 0 && "currently do not filter edge labels, Storage API support needed"); // TODO needs support from the storage
		assert( enumerate && "need careful debugging on range mode"); // TODO needs support from the storage
		assert( join_type == JoinType::INNER && "write all fixmes"); // TODO needs support from the storage
		
	}
	~PhysicalAdjIdxJoin() {}

public:
	
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
	OperatorResultType Execute(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &state) const override;
	OperatorResultType ExecuteNaiveInput(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &state) const;
	OperatorResultType ExecuteRangedInput(ExecutionContext& context, DataChunk &input, DataChunk &chunk, OperatorState &state) const;

	std::string ParamsToString() const override;
	std::string ToString() const override;

	// operator parameters
		// src
	std::string srcName;
	LabelSet srcLabelSet;
		// edge
	LabelSet edgeLabelSet;
	ExpandDirection expandDir;
		// tgt
	LabelSet tgtLabelSet;
		// others
	JoinType join_type;
	bool load_eid;
	bool enumerate;

	// performance counter
	boost::timer::cpu_timer adjfetch_timer;
	bool adjfetch_timer_started;
	int64_t adjfetch_time;

	boost::timer::cpu_timer tgtfetch_timer;
	bool tgtfetch_timer_started;
	int64_t tgtfetch_time;

};

}