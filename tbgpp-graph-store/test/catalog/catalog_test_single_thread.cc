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
//#include "catalog.hpp"
#include "database.hpp"

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

TEST_CASE ("Create Graph", "[catalog]") {
  std::unique_ptr<DuckDB> database;
  database = make_unique<DuckDB>(nullptr);
  Catalog& cat_instance = database->instance->GetCatalog();

  cat_instance.CreateGraph(...);
}

TEST_CASE ("Create Multiple Graphs", "[catalog]") {
  std::unique_ptr<DuckDB> database;
  database = make_unique<DuckDB>(nullptr);
  Catalog& cat_instance = database->instance->GetCatalog();

  std::string graph_name_prefix = "";
  for (int i = 0; i < 1000; i++) {
    std::string graph_name =graph_name_prefix + std::to_string(i);
    cat_instance.CreateGraph(...);
  }
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