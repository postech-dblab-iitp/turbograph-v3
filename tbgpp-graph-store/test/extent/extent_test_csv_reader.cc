#include <string>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/shm.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <thread>
#include <vector>
#include <memory>

#include "common/graph_csv_reader.hpp"
#include "common/types/data_chunk.hpp"
#include "extent/extent_manager.hpp"

#include "catalog/catalog.hpp"
#include "main/database.hpp"
#include "main/client_context.hpp"
#include "parser/parsed_data/create_schema_info.hpp"
#include "parser/parsed_data/create_graph_info.hpp"
#include "parser/parsed_data/create_partition_info.hpp"
#include "parser/parsed_data/create_property_schema_info.hpp"
#include "parser/parsed_data/create_extent_info.hpp"
#include "parser/parsed_data/create_chunkdefinition_info.hpp"
#include "catalog/catalog_entry/list.hpp"
#include "cache/chunk_cache_manager.h"

using namespace duckdb;

#define CATCH_CONFIG_RUNNER
#include <catch2/catch_all.hpp>

typedef boost::interprocess::managed_shared_memory::const_named_iterator const_named_it;

void helper_deallocate_objects_in_shared_memory () {
  string server_socket = "/tmp/catalog_server";
  // setup unix domain socket with storage
  int server_conn_ = socket(AF_UNIX, SOCK_STREAM, 0);
  if (server_conn_ < 0) {
    perror("cannot socket");
    exit(-1);
  }

  struct sockaddr_un addr;
  memset(&addr, 0, sizeof(addr));
  strncpy(addr.sun_path, server_socket.c_str(), server_socket.size());
  addr.sun_family = AF_UNIX;
  int status = connect(server_conn_, (struct sockaddr *)&addr, sizeof(addr));
  if (status < 0) {
    perror("cannot connect to the store");
    exit(-1);
  }

  bool reinitialize_done = false;

  int nbytes_recv = recv(server_conn_, &reinitialize_done, sizeof(bool), 0);
  if (nbytes_recv != sizeof(bool)) {
    perror("error receiving the reinitialize_done bit");
    exit(-1);
  }

  if (!reinitialize_done) {
    std::cerr << "Re-initialize failure!" << std::endl;
    exit(-1);
  }

  fprintf(stdout, "Re-initialize shared memory\n");
}

TEST_CASE ("Json Reader Open File & Read Test", "[tile]") {
  GraphCSVFileReader reader;
  
  reader.InitCSVFile("/home/tslee/turbograph-v3/tbgpp-graph-store/test/extent/comment_0_0.csv", GraphComponentType::VERTEX, '|');
  /*
  // Assume types are given
  DataChunk output;
  vector<LogicalType> types = {LogicalType::UBIGINT, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
  output.Initialize(types);
  vector<string> key_names = {"id", "firstName", "lastName", "gender"};

  // tile manager Create Vertex Tiles

  ExtentManager ext_mng;
  // Create Default catalog
  helper_deallocate_objects_in_shared_memory();
  std::unique_ptr<DuckDB> database;
  database = make_unique<DuckDB>(nullptr);
  Catalog& cat_instance = database->instance->GetCatalog();

  std::shared_ptr<ClientContext> client = 
    std::make_shared<ClientContext>(database->instance->shared_from_this());
  
  CreateSchemaInfo schema_info;
  cat_instance.CreateSchema(*client.get(), &schema_info);

  CreateGraphInfo graph_info("main", "graph1");
  GraphCatalogEntry* graph_cat = (GraphCatalogEntry*) cat_instance.CreateGraph(*client.get(), &graph_info);

  vector<string> vertex_labels = {"label1"};
  CreatePartitionInfo partition_info("main", "partition1");
  PartitionCatalogEntry* partition_cat = (PartitionCatalogEntry*) cat_instance.CreatePartition(*client.get(), &partition_info);
  
  graph_cat->AddVertexPartition(*client.get(), 0, partition_cat->GetOid(), vertex_labels);

  vector<string> property_keys = {"attribute1", "attrubute2"};
  CreatePropertySchemaInfo propertyschema_info("main", "propertyschema1");
  PropertySchemaCatalogEntry* property_schema_cat = (PropertySchemaCatalogEntry*) cat_instance.CreatePropertySchema(*client.get(), &propertyschema_info);

  vector<PropertyKeyID> property_key_ids;
  graph_cat->GetPropertyKeyIDs(*client.get(), property_keys, property_key_ids);
  partition_cat->AddPropertySchema(*client.get(), 0, property_key_ids);
  
  while (!reader.ReadJsonFile(key_names, types, output)) {
    fprintf(stdout, "Read JSON File Ongoing..\n");
    // Create Vertex Extent
    ext_mng.CreateVertexExtent(*client.get(), output, *property_schema_cat);
  }
  fprintf(stdout, "Read JSON File DONE\n");
  //fprintf(stdout, "%s", output.ToString().c_str());*/
}

int main(int argc, char **argv) {
  // Initialize System Parameters
  DiskAioParameters::NUM_THREADS = 1;
  DiskAioParameters::NUM_TOTAL_CPU_CORES = 1;
  DiskAioParameters::NUM_CPU_SOCKETS = 1;
  DiskAioParameters::NUM_DISK_AIO_THREADS = DiskAioParameters::NUM_CPU_SOCKETS * 2;
  DiskAioParameters::WORKSPACE = "/home/tslee/turbograph-v3/tbgpp-graph-store/test/extent/data/";
  
  int res;
  DiskAioFactory* disk_aio_factory = new DiskAioFactory(res, DiskAioParameters::NUM_DISK_AIO_THREADS, 128);
  core_id::set_core_ids(DiskAioParameters::NUM_THREADS);

  // Initialize ChunkCacheManager
  //ChunkCacheManager::ccm = new ChunkCacheManager();

  // Run Catch Test
  int result = Catch::Session().run(argc, argv);

  // Destruct ChunkCacheManager
  delete ChunkCacheManager::ccm;
  return 0;
}
