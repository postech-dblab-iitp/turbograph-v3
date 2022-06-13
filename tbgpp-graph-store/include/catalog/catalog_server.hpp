#ifndef CATALOG_SERVER_H
#define CATALOG_SERVER_H

#include <mutex>
#include <string>
#include <sys/types.h>
#include <unordered_set>
#include <common/boost.hpp>

namespace duckdb {

class CatalogServer {
public:
  CatalogServer(const std::string &unix_socket);
  void Run();

private:
  void monitor();
  void listener();
  bool recreate();

  std::string unix_socket_;
  boost::interprocess::managed_shared_memory *catalog_segment;
};

} // namespace duckdb

#endif // CATALOG_SERVER_H
