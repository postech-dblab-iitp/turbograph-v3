#include "main/client_data.hpp"
#include "main/client_context.hpp"
//#include "main/client_context_file_opener.hpp"
#include "main/query_profiler.hpp"
//#include "common/random_engine.hpp"
//#include "storage/catalog/catalog_entry/schema_catalog_entry.hpp"
//#include "storage/catalog/catalog_search_path.hpp"
//#include "storage/catalog/catalog.hpp"
//#include "common/serializer/buffered_file_writer.hpp"

namespace duckdb {

//ClientData::ClientData(ClientContext &context) : catalog_search_path(make_unique<CatalogSearchPath>(context)) {
ClientData::ClientData(ClientContext &context) {
	physical_op_counter = 0;
	profiler = make_shared<QueryProfiler>(context);
	//query_profiler_history = make_unique<QueryProfilerHistory>();
	//temporary_objects = make_unique<SchemaCatalogEntry>(&Catalog::GetCatalog(context), TEMP_SCHEMA, true);
	//random_engine = make_unique<RandomEngine>();
	//file_opener = make_unique<ClientContextFileOpener>(context);
}
ClientData::~ClientData() {
}

ClientData &ClientData::Get(ClientContext &context) {
	return *context.client_data;
}

/*RandomEngine &RandomEngine::Get(ClientContext &context) {
	return *ClientData::Get(context).random_engine;
}*/

} // namespace duckdb
