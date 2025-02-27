#include "catalog/catalog_manager.hpp"
#include "catalog/catalog_entry/list.hpp"
#include "common/logger.hpp"

namespace duckdb {

typedef fixed_managed_mapped_file::const_named_iterator const_named_it;

std::string CatalogManager::shm_directory_ = "";
fixed_managed_mapped_file *CatalogManager::catalog_segment = nullptr;

void CatalogManager::CreateOrOpenCatalog(std::string shm_directory) {
  SetupLogger();
  spdlog::info("CatalogServer uses Boost {}.{}.{}", BOOST_VERSION / 100000, BOOST_VERSION / 100 % 1000, BOOST_VERSION % 100);
  
  shm_directory_ = shm_directory;
  std::string shm_path = shm_directory_ + std::string("/iTurboGraph_Catalog_SHM");
  catalog_segment = new fixed_managed_mapped_file(boost::interprocess::open_or_create, shm_path.c_str(), 15 * 1024 * 1024 * 1024UL, (void *) SERVER_CATALOG_ADDR);
  spdlog::info("Open/Create shared memory: iTurboGraph_Catalog_SHM");

  const_named_it named_beg = catalog_segment->named_begin();
	const_named_it named_end = catalog_segment->named_end();
  int64_t num_objects_in_the_catalog = 0;
	for(; named_beg != named_end; ++named_beg){
    num_objects_in_the_catalog++;
	}
  spdlog::info("# of named object list in the catalog = {}", num_objects_in_the_catalog);
}

void CatalogManager::CloseCatalog() {
  catalog_segment->flush();
  delete catalog_segment;
  spdlog::info("Close CatalogServer, flushes cached data to file done");
}

} // namespace duckdb
