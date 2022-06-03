#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/shm.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <thread>
#include <vector>
#include <memory>

//#include "chunk_cache_manager.h"
#include "catalog.hpp"
#include "database.hpp"
#include "client_context.hpp"
#include "create_schema_info.hpp"
#include "create_graph_info.hpp"
#include "create_partition_info.hpp"
#include "catalog/catalog_entry/list.hpp"

using namespace duckdb;

#define CATCH_CONFIG_RUNNER
#include <catch2/catch_all.hpp>

bool helper_check_file_exists (const std::string& name) {
  struct stat buffer;
  return (stat (name.c_str(), &buffer) == 0); 
}

TEST_CASE ("Create Catalog Instance", "[catalog]") {
  std::unique_ptr<DuckDB> database;
  database = make_unique<DuckDB>(nullptr);
  
  Catalog& cat_instance = database->instance->GetCatalog();
}

TEST_CASE ("Create a graph catalog", "[catalog]") {
  REQUIRE(true);
  std::unique_ptr<DuckDB> database;
  database = make_unique<DuckDB>(nullptr);
  REQUIRE(true);
  Catalog& cat_instance = database->instance->GetCatalog();
  REQUIRE(true);

  std::shared_ptr<ClientContext> client = 
    std::make_shared<ClientContext>(database->instance->shared_from_this());
  REQUIRE(true);

  CreateSchemaInfo schema_info;
  cat_instance.CreateSchema(*client.get(), &schema_info);

  CreateGraphInfo graph_info("main", "graph1");
  cat_instance.CreateGraph(*client.get(), &graph_info);
}

TEST_CASE ("Create multiple graph catalogs", "[catalog]") {
  std::unique_ptr<DuckDB> database;
  database = make_unique<DuckDB>(nullptr);
  Catalog& cat_instance = database->instance->GetCatalog();

  std::shared_ptr<ClientContext> client = 
    std::make_shared<ClientContext>(database->instance->shared_from_this());

  CreateSchemaInfo schema_info;
  cat_instance.CreateSchema(*client.get(), &schema_info);

  std::string graph_name_prefix = "graph";
  for (int i = 0; i < 1000; i++) {
    std::string graph_name =graph_name_prefix + std::to_string(i);
    CreateGraphInfo graph_info("main", graph_name);
    cat_instance.CreateGraph(*client.get(), &graph_info);
  }
}

TEST_CASE ("Create a graph catalog with a name that already exists", "[catalog]") {
  std::unique_ptr<DuckDB> database;
  database = make_unique<DuckDB>(nullptr);
  Catalog& cat_instance = database->instance->GetCatalog();

  std::shared_ptr<ClientContext> client = 
    std::make_shared<ClientContext>(database->instance->shared_from_this());

  CreateSchemaInfo schema_info;
  cat_instance.CreateSchema(*client.get(), &schema_info);

  CreateGraphInfo graph_info1("main", "graph1");
  cat_instance.CreateGraph(*client.get(), &graph_info1);

  CreateGraphInfo graph_info2("main", "graph1");
  REQUIRE_THROWS(cat_instance.CreateGraph(*client.get(), &graph_info2));
}

TEST_CASE ("Create a vertex partition catalog", "[catalog]") {
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
  
  graph_cat->AddVertexPartition(*client.get(), 0, vertex_labels);
}

TEST_CASE ("Create an edge partition catalog", "[catalog]") {
  std::unique_ptr<DuckDB> database;
  database = make_unique<DuckDB>(nullptr);
  Catalog& cat_instance = database->instance->GetCatalog();

  std::shared_ptr<ClientContext> client = 
    std::make_shared<ClientContext>(database->instance->shared_from_this());
  
  CreateSchemaInfo schema_info;
  cat_instance.CreateSchema(*client.get(), &schema_info);

  CreateGraphInfo graph_info("main", "graph1");
  GraphCatalogEntry* graph_cat = (GraphCatalogEntry*) cat_instance.CreateGraph(*client.get(), &graph_info);

  string edge_type = "type1";
  CreatePartitionInfo partition_info("main", "partition1");
  PartitionCatalogEntry* partition_cat = (PartitionCatalogEntry*) cat_instance.CreatePartition(*client.get(), &partition_info);
  
  graph_cat->AddEdgePartition(*client.get(), 0, edge_type);
}

TEST_CASE ("Create a property schema catalog", "[catalog]") {
}

TEST_CASE ("Create multiple property schemas in a partition catalog", "[catalog]") {
}

TEST_CASE ("Create an extent catalog", "[catalog]") {
}

TEST_CASE ("Create an chunk definition catalogs", "[catalog]") {
  // Create chunk definitions for each type
}

TEST_CASE ("Get extent catalog", "[catalog]") {
}

TEST_CASE ("Change delta store to extent store", "[catalog]") {
}

TEST_CASE ("Create vertex and edge extents", "[catalog]") {
}

TEST_CASE ("Create constraint catalogs", "[catalog]") {
}

TEST_CASE ("Outlier data in a extent", "[catalog]") {
}

TEST_CASE ("Large data test", "[catalog]") {
}

TEST_CASE ("Get catalog information as a graph type", "[catalog]") {
}

TEST_CASE ("Serialize/Deserialize", "[catalog]") {
}

TEST_CASE ("Create local index catalog", "[catalog]") {
}

int main(int argc, char **argv) {
  // Initialize System Parameters
  /*DiskAioParameters::NUM_THREADS = 1;
  DiskAioParameters::NUM_TOTAL_CPU_CORES = 1;
  DiskAioParameters::NUM_CPU_SOCKETS = 1;
  DiskAioParameters::NUM_DISK_AIO_THREADS = DiskAioParameters::NUM_CPU_SOCKETS * 2;
  */
  /*int res;
  DiskAioFactory* disk_aio_factory = new DiskAioFactory(res, DiskAioParameters::NUM_DISK_AIO_THREADS, 128);
  core_id::set_core_ids(DiskAioParameters::NUM_THREADS);
*/
  // Initialize CatalogManager


  // Run Catch Test
  int result = Catch::Session().run(argc, argv);

  return 0;
}