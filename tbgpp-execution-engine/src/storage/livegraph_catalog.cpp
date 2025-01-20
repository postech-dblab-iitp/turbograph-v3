#include <storage/livegraph_catalog.hpp>

#include <iostream>
namespace s62 {
void LiveGraphCatalog::printCatalog() {

	std::cout << "### LiveGrahCatalog ###" << std::endl << std::endl;

	std::cout << "[VertexLabelsetToLiveGraphRangeMapping]" << std::endl << std::endl;
	for( auto item :this->vertexLabelSetToLGRangeMapping ){
		std::cout << item.first << ": (" << item.second.first << ", " << item.second.second << ")" << std::endl;
	}
	
	std::cout << std::endl << std::endl;

	std::cout << "[EdgeLabelSetToLiveGraphEdgeLabelMapping]" << std::endl << std::endl;
	for( auto item: this->edgeLabelSetToLGEdgeLabelMapping ) {
		std::cout << item.first << ": " << item.second << std::endl;
	}

	std::cout << std::endl << std::endl;

}
}