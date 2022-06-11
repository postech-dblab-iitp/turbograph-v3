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
#include "parser/parsed_data/create_schema_info.hpp"
#include "parser/parsed_data/create_graph_info.hpp"
#include "parser/parsed_data/create_partition_info.hpp"
#include "parser/parsed_data/create_property_schema_info.hpp"
#include "parser/parsed_data/create_extent_info.hpp"
#include "parser/parsed_data/create_chunkdefinition_info.hpp"
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
  std::unique_ptr<DuckDB> database;
  database = make_unique<DuckDB>(nullptr);
  Catalog& cat_instance = database->instance->GetCatalog();

  std::shared_ptr<ClientContext> client = 
    std::make_shared<ClientContext>(database->instance->shared_from_this());

  fprintf(stdout, "FUCK1111111\n");
  CreateSchemaInfo schema_info;
  cat_instance.CreateSchema(*client.get(), &schema_info);

  fprintf(stdout, "FUCK1111112\n");
  CreateGraphInfo graph_info("main", "graph1");
  cat_instance.CreateGraph(*client.get(), &graph_info);
  fprintf(stdout, "FUCK1111113\n");
}
/*
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

  vector<string> property_keys = {"attribute1", "attrubute2"};
  CreatePropertySchemaInfo propertyschema_info("main", "propertyschema1");
  PropertySchemaCatalogEntry* property_schema_cat = (PropertySchemaCatalogEntry*) cat_instance.CreatePropertySchema(*client.get(), &propertyschema_info);

  vector<PropertyKeyID> property_key_ids;
  graph_cat->GetPropertyKeyIDs(*client.get(), property_keys, property_key_ids);
  partition_cat->AddPropertySchema(*client.get(), 0, property_key_ids);
}

TEST_CASE ("Create multiple property schemas in a partition catalog", "[catalog]") {
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

  for (int i = 0; i < 100; i++) {
    vector<string> property_keys;
    for (int j = 0; j <= i; j++) {
      property_keys.push_back(std::string("attribute") + std::to_string(j));
    }
    string property_schema_name = "propertyschema" + std::to_string(i);
    CreatePropertySchemaInfo propertyschema_info("main", property_schema_name);
    PropertySchemaCatalogEntry* property_schema_cat = (PropertySchemaCatalogEntry*) cat_instance.CreatePropertySchema(*client.get(), &propertyschema_info);

    vector<PropertyKeyID> property_key_ids;
    graph_cat->GetPropertyKeyIDs(*client.get(), property_keys, property_key_ids);
    partition_cat->AddPropertySchema(*client.get(), 0, property_key_ids);
  }
}

TEST_CASE ("Create an extent catalog", "[catalog]") {
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

  vector<string> property_keys = {"attribute1", "attrubute2"};
  CreatePropertySchemaInfo propertyschema_info("main", "propertyschema1");
  PropertySchemaCatalogEntry* property_schema_cat = (PropertySchemaCatalogEntry*) cat_instance.CreatePropertySchema(*client.get(), &propertyschema_info);

  vector<PropertyKeyID> property_key_ids;
  graph_cat->GetPropertyKeyIDs(*client.get(), property_keys, property_key_ids);
  partition_cat->AddPropertySchema(*client.get(), 0, property_key_ids);

  CreateExtentInfo extent_info("main", "extent1", ExtentType::DELTA, 0);
  ExtentCatalogEntry* extent_cat = (ExtentCatalogEntry*) cat_instance.CreateExtent(*client.get(), &extent_info);
}

TEST_CASE ("Create an chunk definition catalogs", "[catalog]") {
  // Create chunk definitions for each type
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

  vector<string> property_keys = {"attribute1", "attrubute2"};
  CreatePropertySchemaInfo propertyschema_info("main", "propertyschema1");
  PropertySchemaCatalogEntry* property_schema_cat = (PropertySchemaCatalogEntry*) cat_instance.CreatePropertySchema(*client.get(), &propertyschema_info);

  vector<PropertyKeyID> property_key_ids;
  graph_cat->GetPropertyKeyIDs(*client.get(), property_keys, property_key_ids);
  partition_cat->AddPropertySchema(*client.get(), 0, property_key_ids);

  CreateExtentInfo extent_info("main", "extent1", ExtentType::DELTA, 0);
  ExtentCatalogEntry* extent_cat = (ExtentCatalogEntry*) cat_instance.CreateExtent(*client.get(), &extent_info);

  static const LogicalTypeId AllType[] = { 
    LogicalTypeId::SQLNULL,
    LogicalTypeId::UNKNOWN,
    LogicalTypeId::ANY,
    LogicalTypeId::USER,
    LogicalTypeId::BOOLEAN,
    LogicalTypeId::TINYINT,
    LogicalTypeId::SMALLINT,
    LogicalTypeId::INTEGER,
    LogicalTypeId::BIGINT,
    LogicalTypeId::DATE,
    LogicalTypeId::TIME,
    LogicalTypeId::TIMESTAMP_SEC,
    LogicalTypeId::TIMESTAMP_MS,
    LogicalTypeId::TIMESTAMP,
    LogicalTypeId::TIMESTAMP_NS,
    LogicalTypeId::DECIMAL,
    LogicalTypeId::FLOAT,
    LogicalTypeId::DOUBLE,
    LogicalTypeId::CHAR,
    LogicalTypeId::VARCHAR,
    LogicalTypeId::BLOB,
    LogicalTypeId::INTERVAL,
    LogicalTypeId::UTINYINT,
    LogicalTypeId::USMALLINT,
    LogicalTypeId::UINTEGER,
    LogicalTypeId::UBIGINT,
    LogicalTypeId::TIMESTAMP_TZ,
    LogicalTypeId::TIME_TZ,
    LogicalTypeId::JSON,
    LogicalTypeId::HUGEINT,
    LogicalTypeId::POINTER,
    LogicalTypeId::HASH,
    LogicalTypeId::VALIDITY,
    LogicalTypeId::UUID,
    LogicalTypeId::STRUCT,
    LogicalTypeId::LIST,
    LogicalTypeId::MAP,
    LogicalTypeId::TABLE,
    //LogicalTypeId::ENUM, // currently not allowed
    LogicalTypeId::AGGREGATE_STATE,
  };

  int chunk_definition_idx = 0;
  for ( const auto l_type_id : AllType ) {
    LogicalType l_type(l_type_id);
    string chunkdefinition_name = "chunkdefinition" + std::to_string(chunk_definition_idx);
    CreateChunkDefinitionInfo chunkdefinition_info("main", chunkdefinition_name, l_type);
    ChunkDefinitionCatalogEntry* chunkdefinition_cat = (ChunkDefinitionCatalogEntry*) cat_instance.CreateChunkDefinition(*client.get(), &chunkdefinition_info);
    chunk_definition_idx++;
  }
}

TEST_CASE ("Get extent catalog", "[catalog]") {
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

  vector<string> property_keys = {"attribute1", "attrubute2"};
  CreatePropertySchemaInfo propertyschema_info("main", "propertyschema1");
  PropertySchemaCatalogEntry* property_schema_cat = (PropertySchemaCatalogEntry*) cat_instance.CreatePropertySchema(*client.get(), &propertyschema_info);

  vector<PropertyKeyID> property_key_ids;
  graph_cat->GetPropertyKeyIDs(*client.get(), property_keys, property_key_ids);
  partition_cat->AddPropertySchema(*client.get(), 0, property_key_ids);

  for (int i = 0; i < 100; i++) {
    string extent_name = "extent" + std::to_string(i);
    CreateExtentInfo extent_info("main", extent_name, ExtentType::DELTA, i);
    ExtentCatalogEntry* extent_cat = (ExtentCatalogEntry*) cat_instance.CreateExtent(*client.get(), &extent_info);
  }
  for (int i = 0; i < 100; i++) {
    string extent_name = "extent" + std::to_string(i);
    ExtentCatalogEntry* extent_cat = (ExtentCatalogEntry*) cat_instance.GetEntry(*client.get(), CatalogType::EXTENT_ENTRY, "main", extent_name);
  }
}

TEST_CASE ("Change delta store to extent store", "[catalog]") {
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

  vector<string> property_keys = {"attribute1", "attrubute2"};
  CreatePropertySchemaInfo propertyschema_info("main", "propertyschema1");
  PropertySchemaCatalogEntry* property_schema_cat = (PropertySchemaCatalogEntry*) cat_instance.CreatePropertySchema(*client.get(), &propertyschema_info);

  vector<PropertyKeyID> property_key_ids;
  graph_cat->GetPropertyKeyIDs(*client.get(), property_keys, property_key_ids);
  partition_cat->AddPropertySchema(*client.get(), 0, property_key_ids);

  string extent_name = "extent" + std::to_string(0);
  CreateExtentInfo extent_info("main", extent_name, ExtentType::DELTA, 0);
  ExtentCatalogEntry* extent_cat = (ExtentCatalogEntry*) cat_instance.CreateExtent(*client.get(), &extent_info);
  property_schema_cat->AddExtent(extent_cat);

  extent_cat->SetExtentType(ExtentType::EXTENT);
}
*/
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