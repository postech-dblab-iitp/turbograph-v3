#include "main/database.hpp"

#include "catalog/catalog.hpp"
#include "catalog/catalog_wrapper.hpp"
//#include "common/virtual_file_system.hpp"
#include "main/client_context.hpp"
//#include "parallel/task_scheduler.hpp"
//#include "storage/storage_manager.hpp"
//#include "storage/object_cache.hpp"
//#include "transaction/transaction_manager.hpp"
//#include "main/connection_manager.hpp"
//#include "function/compression_function.hpp"
//#include "main/extension_helper.hpp"
#include "common/boost.hpp"

// #include "catalog/catalog_entry/schema_catalog_entry.hpp" 
#include "parser/parsed_data/create_schema_info.hpp"

#ifndef DUCKDB_NO_THREADS
#include "common/thread.hpp"
#endif

namespace s62 {

/*DBConfig::DBConfig() {
	compression_functions = make_unique<CompressionFunctionSet>();
}

DBConfig::~DBConfig() {
}*/

DatabaseInstance::DatabaseInstance() {
}

DatabaseInstance::~DatabaseInstance() {
	delete catalog_shm;

	if (Exception::UncaughtException()) {
		return;
	}

	// shutting down: attempt to checkpoint the database
	// but only if we are not cleaning up as part of an exception unwind
	try {
		/*auto &storage = StorageManager::GetStorageManager(*this);
		if (!storage.InMemory()) {
			auto &config = storage.db.config;
			if (!config.checkpoint_on_shutdown) {
				return;
			}
			storage.CreateCheckpoint(true);
		}*/
	} catch (...) {
	}
}

BufferManager &BufferManager::GetBufferManager(DatabaseInstance &db) {
	return *db.GetStorageManager().buffer_manager;
}

BlockManager &BlockManager::GetBlockManager(DatabaseInstance &db) {
	return *db.GetStorageManager().block_manager;
}

BlockManager &BlockManager::GetBlockManager(ClientContext &context) {
	return BlockManager::GetBlockManager(DatabaseInstance::GetDatabase(context));
}

DatabaseInstance &DatabaseInstance::GetDatabase(ClientContext &context) {
	return *context.db;
}

StorageManager &StorageManager::GetStorageManager(DatabaseInstance &db) {
	return db.GetStorageManager();
}

Catalog &Catalog::GetCatalog(DatabaseInstance &db) {
	return db.GetCatalog();
}

/*FileSystem &FileSystem::GetFileSystem(DatabaseInstance &db) {
	return db.GetFileSystem();
}

DBConfig &DBConfig::GetConfig(DatabaseInstance &db) {
	return db.config;
}
*/

ClientConfig &ClientConfig::GetConfig(ClientContext &context) {
	return context.config;
}

/*
TransactionManager &TransactionManager::Get(ClientContext &context) {
	return TransactionManager::Get(DatabaseInstance::GetDatabase(context));
}

TransactionManager &TransactionManager::Get(DatabaseInstance &db) {
	return db.GetTransactionManager();
}

ConnectionManager &ConnectionManager::Get(DatabaseInstance &db) {
	return db.GetConnectionManager();
}

ConnectionManager &ConnectionManager::Get(ClientContext &context) {
	return ConnectionManager::Get(DatabaseInstance::GetDatabase(context));
}*/

bool startsWith(const std::string &str, const std::string &prefix) {
    return str.size() >= prefix.size() && str.compare(0, prefix.size(), prefix) == 0;
}

void DatabaseInstance::Initialize(const char *path) { //, DBConfig *new_config) {
	/*if (new_config) {
		// user-supplied configuration
		Configure(*new_config);
	} else {
		// default configuration
		DBConfig config;
		Configure(config);
	}
	if (config.temporary_directory.empty() && path) {
		// no directory specified: use default temp path
		config.temporary_directory = string(path) + ".tmp";

		// special treatment for in-memory mode
		if (strcmp(path, ":memory:") == 0) {
			config.temporary_directory = ".tmp";
		}
	}
	if (new_config && !new_config->use_temporary_directory) {
		// temporary directories explicitly disabled
		config.temporary_directory = string();
	}*/

	storage =
	    make_unique<StorageManager>(*this, path ? string(path) : string(), false);

    catalog_shm = new fixed_managed_mapped_file(
        boost::interprocess::open_only,
        (string(path) + "/S62_Catalog_SHM").c_str(),
        (void *)CATALOG_ADDR);
    
	int64_t num_objects_in_catalog = 0;
	const_named_it named_beg = catalog_shm->named_begin();
	const_named_it named_end = catalog_shm->named_end();
	vector<vector<string>> object_names;
	vector<vector<void*>> object_ptrs; // for debugging
	object_names.resize(20);
	object_ptrs.resize(20);
	for(; named_beg != named_end; ++named_beg) {
		// A pointer to the name of the named object
		const void *value = named_beg->value();
		const boost::interprocess::managed_shared_memory::char_type *name = named_beg->name();
		// fprintf(stdout, "%s %p\n", name, value);
		if (startsWith(name, "schemacatalogentry")) { // SchemaCatalogEntry
			object_names[0].push_back(name);
		} else if (startsWith(name, "graph")) { // GraphCatalogEntry
			object_names[1].push_back(name);
		} else if (startsWith(name, "vpart")) { // VertexPartitionCatalogEntry
			object_names[2].push_back(name);
			object_ptrs[2].push_back(const_cast<void*>(value));
		} else if (startsWith(name, "epart")) { // EdgePartitionCatalogEntry
			object_names[3].push_back(name);
		} else if (startsWith(name, "vps")) { // VertexPropertySchemaCatalogEntry
			object_names[4].push_back(name);
			object_ptrs[4].push_back(const_cast<void*>(value));
		} else if (startsWith(name, "eps")) { // EdgePropertySchemaCatalogEntry
			object_names[5].push_back(name);
		} else if (startsWith(name, "ext")) { // ExtentCatalogEntry
			object_names[6].push_back(name);
		} else if (startsWith(name, "cdf")) { // ChunkDefinitionCatalogEntry
			object_names[7].push_back(name);
		} else {
			object_names[8].push_back(name);
		}
		num_objects_in_catalog++;
	}
	// fprintf(stdout, "SchemaCatalogEntry\n");
	// for (idx_t i = 0; i < object_names[0].size(); i++) fprintf(stdout, "\t%s\n", object_names[0][i].c_str());
	// fprintf(stdout, "GraphCatalogEntry\n");
	// for (idx_t i = 0; i < object_names[1].size(); i++) fprintf(stdout, "\t%s\n", object_names[1][i].c_str());
	// fprintf(stdout, "VertexPartitionCatalogEntry\n");
	// for (idx_t i = 0; i < object_names[2].size(); i++) fprintf(stdout, "\t%s %p\n", object_names[2][i].c_str(), object_ptrs[2][i]);
	// fprintf(stdout, "EdgePartitionCatalogEntry\n");
	// for (idx_t i = 0; i < object_names[3].size(); i++) fprintf(stdout, "\t%s\n", object_names[3][i].c_str());
	// fprintf(stdout, "VertexPropertySchemaCatalogEntry\n");
	// for (idx_t i = 0; i < object_names[4].size(); i++) fprintf(stdout, "\t%s %p\n", object_names[4][i].c_str(), object_ptrs[4][i]);
	// fprintf(stdout, "EdgePropertySchemaCatalogEntry\n");
	// for (idx_t i = 0; i < object_names[5].size(); i++) fprintf(stdout, "\t%s\n", object_names[5][i].c_str());
	// fprintf(stdout, "ExtentCatalogEntry\n");
	// for (idx_t i = 0; i < object_names[6].size(); i++) fprintf(stdout, "\t%s\n", object_names[6][i].c_str());
	// fprintf(stdout, "ChunkDefinitionCatalogEntry\n");
	// for (idx_t i = 0; i < object_names[7].size(); i++) fprintf(stdout, "\t%s\n", object_names[7][i].c_str());
	// fprintf(stdout, "Else\n");
	// for (idx_t i = 0; i < object_names[8].size(); i++) fprintf(stdout, "\t%s\n", object_names[8][i].c_str());
	// fprintf(stdout, "Num_objects in catalog = %ld\n", num_objects_in_catalog);
	
	bool create_new_db = (num_objects_in_catalog == 0);
	if (create_new_db) {
		// Make a new catalog
		catalog = make_unique<Catalog>(*this, catalog_shm);
	} else {
		// Load the existing catalog
		catalog = make_unique<Catalog>(*this);
		catalog->LoadCatalog(catalog_shm, object_names, path);
	}
	catalog_wrapper = make_unique<CatalogWrapper>(*this);
	//transaction_manager = make_unique<TransactionManager>(*this);
	//scheduler = make_unique<TaskScheduler>(*this);
	//object_cache = make_unique<ObjectCache>();
	//connection_manager = make_unique<ConnectionManager>();

	// initialize the database
	storage->Initialize();

	// only increase thread count after storage init because we get races on catalog otherwise
	//scheduler->SetThreads(config.maximum_threads);
}
/*
DuckDB::DuckDB(const char *path, DBConfig *new_config) : instance(make_shared<DatabaseInstance>()) {
	instance->Initialize(path, new_config);
	if (instance->config.load_extensions) {
		ExtensionHelper::LoadAllExtensions(*this);
	}
}

DuckDB::DuckDB(const string &path, DBConfig *config) : DuckDB(path.c_str(), config) {
}
*/
DuckDB::DuckDB(DatabaseInstance &instance_p) : instance(instance_p.shared_from_this()) {
}

DuckDB::DuckDB(const char *path) : instance(make_shared<DatabaseInstance>()) {
	instance->Initialize(path);
}

DuckDB::~DuckDB() {
	instance.reset();
}

StorageManager &DatabaseInstance::GetStorageManager() {
	return *storage;
}

Catalog &DatabaseInstance::GetCatalog() {
	return *catalog;
}

CatalogWrapper &DatabaseInstance::GetCatalogWrapper() {
	return *catalog_wrapper;
}

fixed_managed_mapped_file *DatabaseInstance::GetCatalogSHM() {
	return catalog_shm;
}

// TransactionManager &DatabaseInstance::GetTransactionManager() {
// 	return *transaction_manager;
// }

// TaskScheduler &DatabaseInstance::GetScheduler() {
// 	return *scheduler;
// }

// ObjectCache &DatabaseInstance::GetObjectCache() {
// 	return *object_cache;
// }

FileSystem &DatabaseInstance::GetFileSystem() {
	return DEFAULT_LOCAL_FILE_SYSTEM;
	// return *config.file_system;
}

// ConnectionManager &DatabaseInstance::GetConnectionManager() {
// 	return *connection_manager;
// }

FileSystem &DuckDB::GetFileSystem() {
	return instance->GetFileSystem();
}

// Allocator &Allocator::Get(ClientContext &context) {
// 	return Allocator::Get(*context.db);
// }

// Allocator &Allocator::Get(DatabaseInstance &db) {
// 	return db.config.allocator;
// }

/*void DatabaseInstance::Configure(DBConfig &new_config) {
	config.access_mode = AccessMode::READ_WRITE;
	if (new_config.access_mode != AccessMode::UNDEFINED) {
		config.access_mode = new_config.access_mode;
	}
	if (new_config.file_system) {
		config.file_system = move(new_config.file_system);
	} else {
		config.file_system = make_unique<VirtualFileSystem>();
	}
	config.maximum_memory = new_config.maximum_memory;
	if (config.maximum_memory == (idx_t)-1) {
		config.maximum_memory = FileSystem::GetAvailableMemory() * 8 / 10;
	}
	if (new_config.maximum_threads == (idx_t)-1) {
#ifndef DUCKDB_NO_THREADS
		config.maximum_threads = std::thread::hardware_concurrency();
#else
		config.maximum_threads = 1;
#endif
	} else {
		config.maximum_threads = new_config.maximum_threads;
	}
	config.external_threads = new_config.external_threads;
	config.load_extensions = new_config.load_extensions;
	config.force_compression = new_config.force_compression;
	config.allocator = move(new_config.allocator);
	config.checkpoint_wal_size = new_config.checkpoint_wal_size;
	config.use_direct_io = new_config.use_direct_io;
	config.temporary_directory = new_config.temporary_directory;
	config.collation = new_config.collation;
	config.default_order_type = new_config.default_order_type;
	config.default_null_order = new_config.default_null_order;
	config.enable_external_access = new_config.enable_external_access;
	config.replacement_scans = move(new_config.replacement_scans);
	config.initialize_default_database = new_config.initialize_default_database;
	config.disabled_optimizers = move(new_config.disabled_optimizers);
}

DBConfig &DBConfig::GetConfig(ClientContext &context) {
	return context.db->config;
}*/

/*idx_t DatabaseInstance::NumberOfThreads() {
	return scheduler->NumberOfThreads();
}

idx_t DuckDB::NumberOfThreads() {
	return instance->NumberOfThreads();
}

bool DuckDB::ExtensionIsLoaded(const std::string &name) {
	return instance->loaded_extensions.find(name) != instance->loaded_extensions.end();
}
void DuckDB::SetExtensionLoaded(const std::string &name) {
	instance->loaded_extensions.insert(name);
}*/

} // namespace s62
