#ifndef CATALOG_SERVER_H
#define CATALOG_SERVER_H

#include <mutex>
#include <string>
#include <sys/types.h>
#include <unordered_set>
#include <common/boost.hpp>
#include "common/boost_typedefs.hpp"

namespace s62 {

class CatalogServer {
public:
  CatalogServer(const std::string &unix_socket, std::string shm_directory);
  void Run();
  void Exit();

private:
  void monitor();
  void listener();
  bool recreate();

  std::string unix_socket_;
  std::string shm_directory_;
  fixed_managed_mapped_file *catalog_segment;
};

} // namespace s62

#endif // CATALOG_SERVER_H
