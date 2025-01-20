#include <signal.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <thread>
#include <iostream>
#include <stdio.h>
#include <catalog/catalog_server.hpp>
#include <catalog/catalog_entry/list.hpp>

namespace s62 {

// typedef fixed_managed_shared_memory::const_named_iterator const_named_it;
typedef fixed_managed_mapped_file::const_named_iterator const_named_it;

void signal_handler(int sig_number) {
  std::cout << "Capture Ctrl+C" << std::endl;
  exit(0);
}

CatalogServer::CatalogServer(const std::string &unix_socket, std::string shm_directory)
    : unix_socket_(unix_socket), shm_directory_(shm_directory) {
  fprintf(stdout, "CatalogServer uses Boost %d.%d.%d\n", BOOST_VERSION / 100000, BOOST_VERSION / 100 % 1000, BOOST_VERSION % 100);
  // Remove the existing shared memory file
  // boost::interprocess::shared_memory_object::remove("S62_Catalog_SHM");
  // int status = remove("/data/S62_Catalog_SHM");
  
  // Create shared memory
  std::string shm_path = shm_directory_ + std::string("/S62_Catalog_SHM");
  catalog_segment = new fixed_managed_mapped_file(boost::interprocess::open_or_create, shm_path.c_str(), 15 * 1024 * 1024 * 1024UL, (void *) CATALOG_ADDR);
  fprintf(stdout, "Open/Create shared memory: S62_Catalog_SHM\n");
  const_named_it named_beg = catalog_segment->named_begin();
	const_named_it named_end = catalog_segment->named_end();
  int64_t num_objects_in_the_catalog = 0;
	for(; named_beg != named_end; ++named_beg){
    num_objects_in_the_catalog++;
		//A pointer to the name of the named object
		// const boost::interprocess::managed_shared_memory::char_type *name = named_beg->name();
		// fprintf(stdout, "\t%s %p\n", name, named_beg->value());
	}
  fprintf(stdout, "# of named object list in the catalog = %ld\n", num_objects_in_the_catalog);
}

bool CatalogServer::recreate() {
  // Remove the existing shared memory
  // boost::interprocess::shared_memory_object::remove("S62_Catalog_SHM");
  delete catalog_segment;
  std::string shm_path = shm_directory_ + std::string("/S62_Catalog_SHM");
  int status = remove(shm_path.c_str());
  if (status == 0) fprintf(stdout, "Remove the existing SHM file\n");
  else fprintf(stdout, "Remove SHM file error\n");
  
  // Create shared memory
  catalog_segment = new fixed_managed_mapped_file(boost::interprocess::create_only, shm_path.c_str(), 15 * 1024 * 1024 * 1024UL, (void *) CATALOG_ADDR);
  fprintf(stdout, "Re-initialize shared memory: S62_Catalog_SHM\n");
  return true;
}

void CatalogServer::listener() {
  int server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (server_fd < 0) {
    perror("cannot create socket");
    exit(-1);
  }

  struct sockaddr_un addr;
  memset(&addr, 0, sizeof(addr));
  addr.sun_family = AF_UNIX;
  strncpy(addr.sun_path, unix_socket_.c_str(), unix_socket_.size());
  unlink(unix_socket_.c_str());

  int status = bind(server_fd, (struct sockaddr *)&addr, sizeof(addr));
  if (status < 0) {
    perror("cannot bind");
    exit(-1);
  }

  status = listen(server_fd, 0);
  if (status < 0) {
    perror("cannot listen");
    exit(-1);
  }
  while (true) {
    int client_fd = accept(server_fd, nullptr, nullptr);
    if (client_fd < 0) {
      perror("cannot accept");
      exit(-1);
    }
    
    bool reinitialize_done = recreate();
    // bool reinitialize_done;
    // Exit();
    
    int bytes_sent = send(client_fd, &reinitialize_done, sizeof(reinitialize_done), 0);
    if (bytes_sent != sizeof(reinitialize_done)) {
      perror("failure sending the ok bit");
      exit(-1);
    }
  }
}

void CatalogServer::monitor() {
  while (true) {
    // originally used to find crashed clients
    usleep(1000000);
  }
}

void CatalogServer::Run() {
  std::thread monitor_thread = std::thread(&CatalogServer::monitor, this);
  std::thread listener_thread = std::thread(&CatalogServer::listener, this);
  listener_thread.join();
  monitor_thread.join();
}

void CatalogServer::Exit() {
  // Flushes cached data to file
  catalog_segment->flush();
  delete catalog_segment;
  fprintf(stdout, "Exit CatalogServer, flushes cached data to file done\n");

  // const_named_it named_beg = catalog_segment->named_begin();
	// const_named_it named_end = catalog_segment->named_end();
	// fprintf(stdout, "All named object list\n");
	// for(; named_beg != named_end; ++named_beg){
	// 	//A pointer to the name of the named object
	// 	const boost::interprocess::managed_shared_memory::char_type *name = named_beg->name();
	// 	fprintf(stdout, "\t%s %p\n", name, named_beg->value());
	// }
  // std::pair<SchemaCatalogEntry *,std::size_t> ret = catalog_segment->find<SchemaCatalogEntry>("schemacatalogentry_main");
  // SchemaCatalogEntry *schema_cat = ret.first;
  // std::list<CatalogType> cat_list = {CatalogType::GRAPH_ENTRY, CatalogType::PARTITION_ENTRY, CatalogType::PROPERTY_SCHEMA_ENTRY, 
  //                                    CatalogType::EXTENT_ENTRY, CatalogType::CHUNKDEFINITION_ENTRY};
  // schema_cat->Scan(CatalogType::GRAPH_ENTRY,
  //                   [&](CatalogEntry *entry) {
  //                     GraphCatalogEntry *graph_cat_entry = (GraphCatalogEntry *)entry;
  //                     fprintf(stdout, "Graph Entry %p %p\n", graph_cat_entry, &graph_cat_entry->name);
  //                     fprintf(stdout, "Graph Name: %s\n", graph_cat_entry->name.c_str());
  //                   });
  // schema_cat->Scan(CatalogType::PARTITION_ENTRY,
  //                   [&](CatalogEntry *entry) {
  //                     PartitionCatalogEntry *part_cat_entry = (PartitionCatalogEntry *)entry;
  //                     fprintf(stdout, "Partition Entry %p %p\n", part_cat_entry, &part_cat_entry->name);
  //                     fprintf(stdout, "Partition Name: %s\n", part_cat_entry->name.c_str());
  //                   });
  // schema_cat->Scan(CatalogType::PROPERTY_SCHEMA_ENTRY,
  //                   [&](CatalogEntry *entry) {
  //                     PropertySchemaCatalogEntry *ps_cat_entry = (PropertySchemaCatalogEntry *)entry;
  //                     fprintf(stdout, "PropertySchema Entry %p %p\n", ps_cat_entry, &ps_cat_entry->name);
  //                     fprintf(stdout, "PropertySchema Name: %s\n", ps_cat_entry->name.c_str());
  //                   });
  // schema_cat->Scan(CatalogType::EXTENT_ENTRY,
  //                   [&](CatalogEntry *entry) {
  //                     ExtentCatalogEntry *ext_cat_entry = (ExtentCatalogEntry *)entry;
  //                     fprintf(stdout, "Extent Entry %p %p\n", ext_cat_entry, &ext_cat_entry->name);
  //                     fprintf(stdout, "Extent Name: %s\n", ext_cat_entry->name.c_str());
  //                   });
  // schema_cat->Scan(CatalogType::CHUNKDEFINITION_ENTRY,
  //                   [&](CatalogEntry *entry) {
  //                     ChunkDefinitionCatalogEntry *cdf_cat_entry = (ChunkDefinitionCatalogEntry *)entry;
  //                     fprintf(stdout, "ChunkDefinition Entry %p %p\n", cdf_cat_entry, &cdf_cat_entry->name);
  //                     fprintf(stdout, "ChunkDefinition Name: %s\n", cdf_cat_entry->name.c_str());
  //                   });
}

} // namespace s62
