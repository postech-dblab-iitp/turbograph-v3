//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/database.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp" // added
#include "common/mutex.hpp"
#include "common/winapi.hpp"
#include "common/boost.hpp"
#include "common/boost_typedefs.hpp"

#include "common/file_system.hpp"
#include "common/local_file_system.hpp"
#include "storage/storage_manager.hpp"

namespace duckdb {
class Catalog;
class CatalogWrapper;
class ClientContext; // added

class DatabaseInstance : public std::enable_shared_from_this<DatabaseInstance> {
	friend class DuckDB;

public:
	DUCKDB_API DatabaseInstance();
	DUCKDB_API ~DatabaseInstance();

public:
	DUCKDB_API StorageManager &GetStorageManager();
	DUCKDB_API Catalog &GetCatalog();
	DUCKDB_API CatalogWrapper &GetCatalogWrapper();
	DUCKDB_API FileSystem &GetFileSystem();
	fixed_managed_mapped_file *GetCatalogSHM(); 

	idx_t NumberOfThreads();

	DUCKDB_API static DatabaseInstance &GetDatabase(ClientContext &context);

private:
	void Initialize(const char *path);

private:
	unique_ptr<StorageManager> storage;
	unique_ptr<Catalog> catalog;
	unique_ptr<CatalogWrapper> catalog_wrapper;
	fixed_managed_mapped_file *catalog_shm;
};

//! The database object. This object holds the catalog and all the
//! database-specific meta information.
class DuckDB {
public:
	//DUCKDB_API explicit DuckDB(const char *path = nullptr, DBConfig *config = nullptr);
	DUCKDB_API explicit DuckDB(const char *path = nullptr);
	//DUCKDB_API explicit DuckDB(const string &path, DBConfig *config = nullptr);
	DUCKDB_API explicit DuckDB(DatabaseInstance &instance);

	DUCKDB_API ~DuckDB();

	//! Reference to the actual database instance
	shared_ptr<DatabaseInstance> instance;

public:
	template <class T>
	void LoadExtension() {
		T extension;
		if (ExtensionIsLoaded(extension.Name())) {
			return;
		}
		extension.Load(*this);
		SetExtensionLoaded(extension.Name());
	}

	DUCKDB_API FileSystem &GetFileSystem();

	DUCKDB_API idx_t NumberOfThreads();
	DUCKDB_API static const char *SourceID();
	DUCKDB_API static const char *LibraryVersion();
	DUCKDB_API static string Platform();
	DUCKDB_API bool ExtensionIsLoaded(const std::string &name);
	DUCKDB_API void SetExtensionLoaded(const std::string &name);
};

} // namespace duckdb
