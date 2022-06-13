#include <signal.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <thread>
#include <iostream>
#include <catalog/catalog_server.hpp>

namespace duckdb {

void signal_handler(int sig_number) {
  std::cout << "Capture Ctrl+C" << std::endl;
  exit(0);
}

CatalogServer::CatalogServer(const std::string &unix_socket)
    : unix_socket_(unix_socket) {
  fprintf(stdout, "CatalogServer uses Boost %d.%d.%d\n", BOOST_VERSION / 100000, BOOST_VERSION / 100 % 1000, BOOST_VERSION % 100);
  //Remove the existing shared memory
  boost::interprocess::shared_memory_object::remove("iTurboGraph_Catalog_SHM");
  
  //Create shared memory
  boost::interprocess::managed_shared_memory *catalog_shm = new boost::interprocess::managed_shared_memory(boost::interprocess::create_only, "iTurboGraph_Catalog_SHM", 1024 * 1024 * 1024);
  fprintf(stdout, "Create shared memory: iTurboGraph_Catalog_SHM\n");
}

bool CatalogServer::recreate() {
  //Remove the existing shared memory
  boost::interprocess::shared_memory_object::remove("iTurboGraph_Catalog_SHM");
  
  //Create shared memory
  boost::interprocess::managed_shared_memory *catalog_shm = new boost::interprocess::managed_shared_memory(boost::interprocess::create_only, "iTurboGraph_Catalog_SHM", 1024 * 1024 * 1024);
  fprintf(stdout, "Re-initialize shared memory: iTurboGraph_Catalog_SHM\n");
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

} // namespace duckdb
