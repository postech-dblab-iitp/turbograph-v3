#include "livegraph.hpp"
#include "storage/livegraph_catalog.hpp"

class GraphStore { 

	// define APIs here
};

class LiveGraphStore: GraphStore {

	LiveGraphStore(livegraph::Graph graph, LiveGraphCatalog catalog);
};