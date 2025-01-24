#pragma once

#include "typedef.hpp"
//#include "livegraph.hpp"

#include <vector>
#include <map>
namespace duckdb {
class LiveGraphCatalog {

public:
	void printCatalog();

public:
	std::vector<std::pair<LabelSet, long>> edgeLabelSetToLGEdgeLabelMapping;					// (IS_LOCATED_AT,) -> 1
	std::vector<std::pair<LabelSet, std::pair<long, long>>> vertexLabelSetToLGRangeMapping;	// (Person) -> <1000, 1010>

};
}