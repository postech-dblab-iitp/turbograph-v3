#include "storage/storage_manager.hpp"
#include "storage/in_memory_block_manager.hpp"
// #include "storage/single_file_block_manager.hpp"
#include "storage/object_cache.hpp"

#include "catalog/catalog.hpp"
#include "common/file_system.hpp"
#include "main/database.hpp"
// #include "main/connection.hpp"
#include "main/client_context.hpp"
#include "function/function.hpp"
#include "parser/parsed_data/create_schema_info.hpp"
// #include "transaction/transaction_manager.hpp"
#include "common/serializer/buffered_file_reader.hpp"
// #include "storage/checkpoint_manager.hpp"

namespace duckdb {

StorageManager::StorageManager(DatabaseInstance &db, string path, bool read_only)
    : db(db), path(move(path)), read_only(read_only) {
}

StorageManager::~StorageManager() {
}

StorageManager &StorageManager::GetStorageManager(ClientContext &context) {
	return StorageManager::GetStorageManager(*context.db);
}

BufferManager &BufferManager::GetBufferManager(ClientContext &context) {
	return BufferManager::GetBufferManager(*context.db);
} 

// ObjectCache &ObjectCache::GetObjectCache(ClientContext &context) {
// 	return context.db->GetObjectCache();
// }

bool ObjectCache::ObjectCacheEnabled(ClientContext &context) {
	// TODO always disable
	return false;
	// return context.db->config.object_cache_enable;
}

bool StorageManager::InMemory() {
	return path.empty() || path == ":memory:";
}

void StorageManager::Initialize() {
	// bool in_memory = InMemory();
	bool in_memory = true;
	if (in_memory && read_only) {
		throw CatalogException("Cannot launch in-memory database in read-only mode!");
	}

	// first initialize the base system catalogs
	// these are never written to the WAL
	// Connection con(db);
	// con.BeginTransaction();

	// auto &config = DBConfig::GetConfig(db);
	// auto &catalog = Catalog::GetCatalog(*con.context);

	// // commit transactions
	// con.Commit();

	if (!in_memory) {
		// create or load the database from disk, if not in-memory mode
		// LoadDatabase();
	} else {
		block_manager = make_unique<InMemoryBlockManager>();
		// Only load buffermanager and blockamanger
		std::string temp_directory = "/tmp/.tmp";
		idx_t maximum_memory = 107374182400; // 100GB
		// idx_t maximum_memory = 107374182400; // 100GB
		// buffer_manager = make_unique<BufferManager>(db, config.temporary_directory, config.maximum_memory);
		buffer_manager = make_unique<BufferManager>(db, temp_directory, maximum_memory);
	}
}

void StorageManager::LoadDatabase() {
	// string wal_path = path + ".wal";
	// auto &fs = db.GetFileSystem();
	// auto &config = db.config;
	// bool truncate_wal = false;
	// // first check if the database exists
	// if (!fs.FileExists(path)) {
	// 	if (read_only) {
	// 		throw CatalogException("Cannot open database \"%s\" in read-only mode: database does not exist", path);
	// 	}
	// 	// check if the WAL exists
	// 	if (fs.FileExists(wal_path)) {
	// 		// WAL file exists but database file does not
	// 		// remove the WAL
	// 		fs.RemoveFile(wal_path);
	// 	}
	// 	// initialize the block manager while creating a new db file
	// 	block_manager = make_unique<SingleFileBlockManager>(db, path, read_only, true, config.use_direct_io);
	// 	buffer_manager = make_unique<BufferManager>(db, config.temporary_directory, config.maximum_memory);
	// } else {
	// 	// initialize the block manager while loading the current db file
	// 	auto sf_bm = make_unique<SingleFileBlockManager>(db, path, read_only, false, config.use_direct_io);
	// 	auto sf = sf_bm.get();
	// 	block_manager = move(sf_bm);
	// 	buffer_manager = make_unique<BufferManager>(db, config.temporary_directory, config.maximum_memory);
	// 	sf->LoadFreeList();

	// 	//! Load from storage
	// 	CheckpointManager checkpointer(db);
	// 	checkpointer.LoadFromStorage();
	// 	// check if the WAL file exists
	// 	// if (fs.FileExists(wal_path)) {
	// 	// 	// replay the WAL
	// 	// 	truncate_wal = WriteAheadLog::Replay(db, wal_path);
	// 	// }
	// }
	// initialize the WAL file
	// if (!read_only) {
	// 	wal.Initialize(wal_path);
	// 	if (truncate_wal) {
	// 		wal.Truncate(0);
	// 	}
	// }
}

void StorageManager::CreateCheckpoint(bool delete_wal, bool force_checkpoint) {
	return;
	// if (InMemory() || read_only || !wal.initialized) {
	// 	return;
	// }
	// if (wal.GetWALSize() > 0 || db.config.force_checkpoint || force_checkpoint) {
	// 	// we only need to checkpoint if there is anything in the WAL
	// 	CheckpointManager checkpointer(db);
	// 	checkpointer.CreateCheckpoint();
	// }
	// if (delete_wal) {
	// 	wal.Delete();
	// }
}

} // namespace duckdb
