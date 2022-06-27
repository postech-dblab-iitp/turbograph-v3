#include "typedef.hpp"

#include "execution/physical_operator/cypher_physical_operator.hpp"

using namespace std;
namespace duckdb {

class Expand: public CypherPhysicalOperator {

public:
	Expand(CypherSchema& sch,
		std::string srcName, LabelSet srcEdgeLabelSet, ExpandDirection expandDir, std::string edgeName, LabelSet tgtLabelSet, std::vector<LabelSet> tgtEdgeLabelSets, LoadAdjListOption tgtLoadAdjOpt, PropertyKeys tgtPropertyKeys)
		: CypherPhysicalOperator(sch),
		srcName(srcName), srcEdgeLabelSet(srcEdgeLabelSet), expandDir(expandDir), edgeName(edgeName), tgtLabelSet(tgtLabelSet), tgtEdgeLabelSets(tgtEdgeLabelSets), tgtLoadAdjOpt(tgtLoadAdjOpt), tgtPropertyKeys(tgtPropertyKeys)
		 {

		assert( expandDir == ExpandDirection::OUTGOING && "currently supports outgoing index");
	}
	~Expand() {}

public:
	
	unique_ptr<OperatorState> GetOperatorState() const override;
	OperatorResultType Execute(GraphStore* graph, DataChunk &input, DataChunk &chunk, OperatorState &state) const override;

	std::string ParamsToString() const override;
	std::string ToString() const override;

	// operator parameters
		// src
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
};
}