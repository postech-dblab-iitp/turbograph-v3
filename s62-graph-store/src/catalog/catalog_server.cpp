#include <signal.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <thread>
#include <iostream>
#include <stdio.h>
#include <catalog/catalog_server.hpp>
#include <catalog/catalog_entry/list.hpp>

namespace s62 {

typedef fixed_managed_mapped_file::const_named_iterator const_named_it;

void signal_handler(int sig_number) {
  std::cout << "Capture Ctrl+C" << std::endl;
  exit(0);
}

CatalogServer::CatalogServer(const std::string &unix_socket, std::string shm_directory)
    : unix_socket_(unix_socket), shm_directory_(shm_directory) {
  fprintf(stdout, "CatalogServer uses Boost %d.%d.%d\n", BOOST_VERSION / 100000, BOOST_VERSION / 100 % 1000, BOOST_VERSION % 100);
  
  // Create shared memory
  std::string shm_path = shm_directory_ + std::string("/S62_Catalog_SHM");
  catalog_segment = new fixed_managed_mapped_file(boost::interprocess::open_or_create, shm_path.c_str(), 15 * 1024 * 1024 * 1024UL, (void *) CATALOG_ADDR);
  fprintf(stdout, "Open/Create shared memory: S62_Catalog_SHM\n");
  const_named_it named_beg = catalog_segment->named_begin();
	const_named_it named_end = catalog_segment->named_end();
  int64_t num_objects_in_the_catalog = 0;
	for(; named_beg != named_end; ++named_beg){
    num_objects_in_the_catalog++;
	}
  fprintf(stdout, "# of named object list in the catalog = %ld\n", num_objects_in_the_catalog);
}

bool CatalogServer::recreate() {
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
}

} // namespace s62
