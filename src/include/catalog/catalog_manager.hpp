#ifndef CATALOG_SERVER_H
#define CATALOG_SERVER_H

#include <mutex>
#include <string>
#include <sys/types.h>
#include <unordered_set>
#include <common/boost.hpp>
#include "common/boost_typedefs.hpp"

namespace duckdb {

class CatalogManager {
public:
  static void CreateOrOpenCatalog(std::string shm_directory);
  static void CloseCatalog();

private:

  static std::string shm_directory_;
  static fixed_managed_mapped_file *catalog_segment;
};

} // namespace duckdb

#endif // CATALOG_SERVER_H
