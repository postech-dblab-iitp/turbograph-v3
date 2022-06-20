#include "livegraph.hpp"

#include <iostream>
#include <cassert> 
#include <filesystem>

#include "storage/livegraph_catalog.hpp"


void LDBCInsert(livegraph::Graph& graph, LiveGraphCatalog& catalog, std::string ldbc_path);