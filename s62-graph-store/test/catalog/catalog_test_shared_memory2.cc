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

//#include "chunk_cache_manager.h"
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

using namespace s62;

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

bool helper_check_file_exists (const std::string& name) {
  struct stat buffer;
  return (stat (name.c_str(), &buffer) == 0); 
}

TEST_CASE ("Create a vertex partition catalog", "[catalog]") {
  boost::interprocess::managed_shared_memory catalog_shm(boost::interprocess::open_only, "S62_Catalog_SHM");

  const_named_it named_beg = catalog_shm.named_begin();
	const_named_it named_end = catalog_shm.named_end();
	fprintf(stdout, "All named object list\n");
	for(; named_beg != named_end; ++named_beg){
		//A pointer to the name of the named object
		const boost::interprocess::managed_shared_memory::char_type *name = named_beg->name();
		fprintf(stdout, "\t%s\n", name);
	}

  auto graph_cat_in_shm = catalog_shm.find<GraphCatalogEntry>("graph1");
  GraphCatalogEntry *graph_cat = graph_cat_in_shm.first;
  size_t num_objects = graph_cat_in_shm.second;

  fprintf(stdout, "Num Graph CatalogEntry named graph1 = %lu\n", num_objects);

  for (auto &kv : graph_cat->vertexlabel_map) {
    fprintf(stdout, "%s : %lu\n", kv.first.c_str(), kv.second);
  }
  int x;
  std::cin >> x;
}

int main(int argc, char **argv) {
  // Run Catch Test
  int result = Catch::Session().run(argc, argv);

  return 0;
}