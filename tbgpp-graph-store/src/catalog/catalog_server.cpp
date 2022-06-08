#include <thread>
#include <common/boost.hpp>
#include <catalog/catalog_server.hpp>

namespace duckdb {

CatalogServer::CatalogServer() {
  //Remove shared memory on construction and destruction
  struct shm_remove
  {
    shm_remove() { shared_memory_object::remove("iTurboGraph_Catalog_SHM"); }
    ~shm_remove(){ shared_memory_object::remove("iTurboGraph_Catalog_SHM"); }
  } remover;

  //Create shared memory
  managed_shared_memory segment(create_only, "iTurboGraph_Catalog_SHM", 1024 * 1024 * 1024);
}

void CatalogServer::listener() {
}

void CatalogServer::monitor() {
}

void CatalogServer::Run() {
  std::thread monitor_thread = std::thread(&CatalogServer::monitor, this);
  std::thread listener_thread = std::thread(&CatalogServer::listener, this);
  listener_thread.join();
  monitor_thread.join();
}

int main() {
  CatalogServer cat_server;
  cat_server.Run();

  return 0;
}

} // namespace duckdb
