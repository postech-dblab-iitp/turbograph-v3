#include <memory>
#include <string>
#include "capi_internal.hpp"
#include "main/database.hpp"
#include "cache/chunk_cache_manager.h"
#include "catalog/catalog_wrapper.hpp"
#include "planner/planner.hpp"
#include "main/client_context.hpp"
#include "main/database.hpp"
#include "tbgppdbwrappers.hpp"
#include "CypherLexer.h"
#include "kuzu/parser/transformer.h"
#include "kuzu/binder/binder.h"
#include "gpopt/mdcache/CMDCache.h"

using namespace duckdb;

// Database
static std::unique_ptr<DuckDB> database;
static std::shared_ptr<ClientContext> client;
static s62::Planner* planner;

// Error Handling
static s62_error_code last_error_code = S62_NO_ERROR;
static std::string last_error_message;

s62_state s62_connect(const char *dbname) {
    try
    {
        // Setup configurations
        DiskAioParameters config;
        config.WORKSPACE = dbname;
        config.NUM_THREADS = 1;
        config.NUM_TOTAL_CPU_CORES = 1;
        config.NUM_CPU_SOCKETS = 1;
        config.NUM_DISK_AIO_THREADS = config.NUM_CPU_SOCKETS * 2;

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
        planner_config.INDEX_JOIN_ONLY = true;
        planner = new s62::Planner(planner_config, s62::MDProviderType::TBGPP, client.get());

        // Print done
        std::cout << "Database Connected" << std::endl;
        
        return S62_SUCCESS;
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
        last_error_message = e.what();
        last_error_code = S62_ERROR_CONNECTION_FAILED;

        return S62_ERROR;
    }
}

void s62_disconnect() {
    delete ChunkCacheManager::ccm;
    delete planner;
    database.reset();
    client.reset();
    std::cout << "Database Disconnected" << std::endl;
}

s62_state s62_is_connected() {
    if (database != nullptr) {
        return S62_SUCCESS;
    } else {
        return S62_ERROR;
    }
}

s62_error_code s62_get_last_error(char *errmsg) {
    errmsg = (char*)last_error_message.c_str();
    return last_error_code;
}

s62_version s62_get_version() {
    return "0.0.1";
}

s62_prepared_statement* s62_prepare(Query query) {
	auto wrapper = new PreparedStatementWrapper();
    return nullptr;
}

s62_state s62_bind_value(s62_prepared_statement*, int32_t param_idx, int value) {
    return S62_SUCCESS;
}

s62_state s62_execute(s62_prepared_statement* prep_query) {
    return S62_SUCCESS;
}

int s62_fetch(s62_prepared_statement* prep_query, int* value) {
    static int i = 0;
    if (i < 100 ) {
        *value = i;
        i++;
        return 1;
    } else {
        return 0;
    }
}