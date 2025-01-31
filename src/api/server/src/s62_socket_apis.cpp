#include "api/server/s62_socket_apis.hpp"
#include "main/database.hpp"
#include "storage/cache/chunk_cache_manager.h"
#include "catalog/catalog_wrapper.hpp"
#include "planner/planner.hpp"
#include "main/client_context.hpp"
#include "main/database.hpp"
#include "optimizer/orca/gpopt/tbgppdbwrappers.hpp"
#include "CypherLexer.h"
#include "kuzu/parser/transformer.h"
#include "kuzu/binder/binder.h"
#include "gpopt/mdcache/CMDCache.h"

using namespace antlr4;
using namespace gpopt;

namespace duckdb {

typedef vector<idx_t> PartitionIndexes;
typedef vector<idx_t> PropertySchemaIDs;
typedef vector<PartitionIndexes> PartitionIndexesList;
typedef vector<std::string> Labels;
typedef vector<std::string> Types;

S62SocketAPIs::S62SocketAPIs(std::string& workspace) {
    SetupDefaultConfig();
	config.WORKSPACE = workspace;
    SetupDatabase();
}

S62SocketAPIs::S62SocketAPIs(DiskAioParameters& config): config(config) {
    SetupDatabase();
}

S62SocketAPIs::~S62SocketAPIs(){
  	delete ChunkCacheManager::ccm;
    std::cout << "Database Disconnected" << std::endl;
}

void S62SocketAPIs::SetupDefaultConfig() {
    config.NUM_THREADS = 1;
    config.NUM_TOTAL_CPU_CORES = 1;
    config.NUM_CPU_SOCKETS = 1;
    config.NUM_DISK_AIO_THREADS = config.NUM_CPU_SOCKETS * 2;
}

void S62SocketAPIs::SetupDatabase() {
    // create disk aio factory
    int res;
	DiskAioFactory* disk_aio_factory = new DiskAioFactory(res, DiskAioParameters::NUM_DISK_AIO_THREADS, 128);
	core_id::set_core_ids(config.NUM_THREADS);

    // create db
    database = std::make_unique<DuckDB>(config.WORKSPACE.c_str());

    // create cache manager
	ChunkCacheManager::ccm = new ChunkCacheManager(config.WORKSPACE.c_str());

    // craet client
	client = std::make_shared<ClientContext>(database->instance->shared_from_this());
    duckdb::SetClientWrapper(client, make_shared<CatalogWrapper>( database->instance->GetCatalogWrapper()));

    // create planner
    auto planner_config = s62::PlannerConfig();
    planner_config.JOIN_ORDER_TYPE = s62::PlannerConfig::JoinOrderType::JOIN_ORDER_EXHAUSTIVE_SEARCH;
    planner_config.DEBUG_PRINT = false;
    planner = new s62::Planner(planner_config, s62::MDProviderType::TBGPP, client.get());

    // Print done
    std::cout << "Database Connected" << std::endl;
}

void S62SocketAPIs::CompileQuery(std::string& query) {
    auto inputStream = ANTLRInputStream(query);
    auto cypherLexer = CypherLexer(&inputStream);
    auto tokens = CommonTokenStream(&cypherLexer);
    tokens.fill();

    auto kuzuCypherParser = kuzu::parser::KuzuCypherParser(&tokens);
    kuzu::parser::Transformer transformer(*kuzuCypherParser.oC_Cypher());
    auto statement = transformer.transform();
    
    auto binder = kuzu::binder::Binder(client.get());
    auto boundStatement = binder.bind(*statement);
    kuzu::binder::BoundStatement * bst = boundStatement.get();

	planner->execute(bst);
}

void S62SocketAPIs::ExecuteStatement(std::string& query, QueryResultSetWrapper& query_result_set_wrapper) {
    CompileQuery(query);
    auto executors = planner->genPipelineExecutors();
    if (executors.size() == 0) { 
        std::cout << "Plan empty" << std::endl; 
        return;
    }
    else {
        for( auto exec : executors ) { exec->ExecutePipeline(); }
        std::cout << "Storing metadata" << std::endl;
        auto& output_schema = executors[executors.size()-1]->pipeline->GetSink()->schema;
        auto property_names = planner->getQueryOutputColNames();
        query_result_set_wrapper.SetPropertyNames(property_names);

        std::cout << "Plan executed" << std::endl;
        query_result_set_wrapper.SetResultChunks(*(executors.back()->context->query_results));
    }
}
}