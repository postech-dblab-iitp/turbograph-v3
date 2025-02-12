
#include "main/capi/s62.h"
#include "storage/cache/chunk_cache_manager.h"
#include "main/database.hpp"
#include "main/client_context.hpp"
#include "catalog/catalog_wrapper.hpp"
#include "optimizer/orca/gpopt/tbgppdbwrappers.hpp"
#include <stdio.h>

using namespace duckdb;

void InitializeDiskAIO(string workspace) {
	DiskAioParameters::NUM_THREADS = 32;
	DiskAioParameters::NUM_TOTAL_CPU_CORES = 32;
	DiskAioParameters::NUM_CPU_SOCKETS = 2;
	DiskAioParameters::NUM_DISK_AIO_THREADS = DiskAioParameters::NUM_CPU_SOCKETS * 2;
	DiskAioParameters::WORKSPACE = workspace;

	int res;
	DiskAioFactory* disk_aio_factory = new DiskAioFactory(res, DiskAioParameters::NUM_DISK_AIO_THREADS, 128);
	core_id::set_core_ids(DiskAioParameters::NUM_THREADS);
}

int main() {
    const char *workspace = "/data/ldbc/sf1/";
    s62_connect(workspace);
    // InitializeDiskAIO(workspace);
	// ChunkCacheManager::ccm = new ChunkCacheManager(workspace);
	// std::unique_ptr<DuckDB> database = make_unique<DuckDB>(workspace);
    // auto client = std::make_shared<ClientContext>(database->instance->shared_from_this());
    // duckdb::SetClientWrapper(client, std::make_shared<CatalogWrapper>(client->db->GetCatalogWrapper()));

	// s62_connect_with_client_context(&client);
    printf("s62_connect() done\n");

    s62_prepared_statement* prep_stmt = s62_prepare("MATCH (n:Tag) RETURN n._id, n.id");
    printf("s62_prepare() done\n");

    s62_resultset_wrapper *result_set_wrp;
    s62_num_rows rows = s62_execute(prep_stmt, &result_set_wrp);
    printf("s62_execute() done\n");
    s62_resultset_wrapper *result_set_wrp2;
    s62_num_rows rows2 = s62_execute(prep_stmt, &result_set_wrp2);
    printf("s62_execute() done\n");

    s62_disconnect();

    return 0;
}