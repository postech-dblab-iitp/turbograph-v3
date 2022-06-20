#include <storage/livegraph_catalog.hpp>

#include <iostream>

void LiveGraphCatalog::printCatalog() {

	std::cout << "### LiveGrahCatalog ###" << std::endl << std::endl;

	std::cout << "[VertexLabelsetToLiveGraphRangeMapping]" << std::endl << std::endl;
	for( auto item :this->vertexLabelSetToLGRangeMapping ){
		std::cout << item.first << ": (" << item.second.first << ", " << item.second.second << ")" << std::endl;
	}

	// TODO for edges

	std::cout << std::endl << std::endl;

}