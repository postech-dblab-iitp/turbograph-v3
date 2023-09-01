#include "parallel/graph_partition.hpp"
#include <iostream>
#include "main/database.hpp"
#include "main/client_context.hpp"
#include "catalog/catalog.hpp"
#include <mpi.h>
#include "extent/extent_manager.hpp"
#include "main/database.hpp"
#include "main/client_context.hpp"
#include "extent/extent_manager.hpp"
#include "extent/extent_iterator.hpp"
#include "index/index.hpp"
#include "index/art/art.hpp"
#include "cache/chunk_cache_manager.h"
#include "catalog/catalog.hpp"
#include "parser/parsed_data/create_schema_info.hpp"
#include "parser/parsed_data/create_graph_info.hpp"
#include "parser/parsed_data/create_partition_info.hpp"
#include "parser/parsed_data/create_property_schema_info.hpp"
#include "parser/parsed_data/create_extent_info.hpp"
#include "parser/parsed_data/create_chunkdefinition_info.hpp"
#include "catalog/catalog_entry/list.hpp"
#include "common/graph_csv_reader.hpp"
#include "common/graph_simdcsv_parser.hpp"
#include "storage/graph_store.hpp"
#include "storage/ldbc_insert.hpp"
#include "storage/livegraph_catalog.hpp"
#include <utility>

using namespace duckdb;

int main(int argc, char** argv)
{
    MPI_Init(&argc, &argv);

    InputParser_ input(argc, argv, 2);
    std::pair<std::string, std::string> fileinfo = input.getCmdOption();

	DiskAioParameters::NUM_THREADS = 1;
	DiskAioParameters::NUM_TOTAL_CPU_CORES = 1;
	DiskAioParameters::NUM_CPU_SOCKETS = 1;
	DiskAioParameters::NUM_DISK_AIO_THREADS = DiskAioParameters::NUM_CPU_SOCKETS * 2;

    Aio_Helper::Initialize(DEFAULT_AIO_THREAD); 
    std::thread* rr_receiver_ = new std::thread(RequestRespond::ReceiveRequest);
    PartitionStatistics::init();
    RequestRespond::Initialize(4 * 1024 * 1024L, 128, 2 * 1024 * 1024 * 1024L, DEFAULT_NIO_BUFFER_SIZE, DEFAULT_NIO_THREADS, 0);
    RequestRespond::SetMaxDynamicBufferSize(1 * (1024 * 1024 * 1024L));
    GraphPartitioner graphpartitioner;

    int process_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &process_rank);
    std::string file_path_for_this_process(argv[1]);
    file_path_for_this_process += "/";
    file_path_for_this_process += std::to_string(process_rank); 
    graphpartitioner.output_path = file_path_for_this_process;
	DiskAioParameters::WORKSPACE = graphpartitioner.output_path;


	ChunkCacheManager::ccm = new ChunkCacheManager(graphpartitioner.output_path.c_str());
	std::unique_ptr<DuckDB> database;
	database = make_unique<DuckDB>(graphpartitioner.output_path.c_str());
	
	// Initialize ClientContext
	std::shared_ptr<ClientContext> client = 
		std::make_shared<ClientContext>(database->instance->shared_from_this());

	Catalog& cat_instance = database->instance->GetCatalog();
    ExtentManager ext_mng;

    CreateGraphInfo graph_info(DEFAULT_SCHEMA, "graph1");
	GraphCatalogEntry* graph_cat = (GraphCatalogEntry*) cat_instance.CreateGraph(*client.get(), &graph_info);


    graphpartitioner.InitializePartitioner(fileinfo);

    if(graphpartitioner.process_rank == 0) {
        printf("Initialized partitioner.\n");
    }
    DistributionPolicy policy = DistributionPolicy::DIST_HASH; //TODO: implement random dist.

    std::vector<std::string> keynames; //TODO: currently creating keynames manually. Need to change this to be given as argument.
    keynames.push_back("id");


    graphpartitioner.ProcessPartitioning(policy, keynames, cat_instance, ext_mng, client, graph_cat, fileinfo);
    if(graphpartitioner.process_rank == 0) {
        printf("Process partitioning done.\n");
    }

    // graphpartitioner.DistributePartitionedFile();
    // if(graphpartitioner.process_rank == 0) {
    //     printf("Distributing process done..\n");
    // }

    rr_receiver_->join();
    graphpartitioner.ClearPartitioner();
    if(graphpartitioner.process_rank == 0) {
        printf("Partitioner cleaning done.\n");
    }

    delete ChunkCacheManager::ccm;
    MPI_Finalize();
    return 0;
}