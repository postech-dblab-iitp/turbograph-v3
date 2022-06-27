#include "typedef.hpp"

#include "execution/physical_operator/cypher_physical_operator.hpp"

#include <vector>

using namespace std;

namespace dudkdb {

class SimpleProjection: public CypherPhysicalOperator {

public:
	SimpleProjection(CypherSchema& sch, std::vector<int> colOrdering)
		: CypherPhysicalOperator(sch), colOrdering(colOrdering) { }
	~SimpleProjection() {}

public:

	unique_ptr<OperatorState> GetOperatorState() const override;
	OperatorResultType Execute(GraphStore* graph, DataChunk &input, DataChunk &chunk, OperatorState &state) const override;

	std::string ParamsToString() const override;
	std::string ToString() const override;
	
	std::vector<int> colOrdering;


};

}