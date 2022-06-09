//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog_entry.hpp"
#include "catalog/default/default_generator.hpp"
#include "common/common.hpp"
#include "common/case_insensitive_map.hpp"
#include "common/pair.hpp"
#include "common/unordered_set.hpp"
#include "common/mutex.hpp"
#include "common/boost.hpp"
#include "parser/column_definition.hpp"
#include "catalog/catalog_entry/table_catalog_entry.hpp"
#include "catalog/catalog_entry/sequence_catalog_entry.hpp"
//#include "transaction/transaction.hpp"
#include <functional>
#include <memory>

namespace duckdb {
struct AlterInfo;

class ClientContext;

typedef unordered_map<CatalogSet *, unique_lock<mutex>> set_lock_map_t;

typedef boost::interprocess::managed_shared_memory::segment_manager segment_manager_t;
typedef boost::interprocess::allocator<transaction_t, segment_manager_t> transaction_t_allocator;
typedef boost::interprocess::allocator<bool, segment_manager_t> bool_allocator;
typedef boost::interprocess::allocator<idx_t, segment_manager_t> idx_t_allocator;
typedef boost::interprocess::allocator<char, segment_manager_t> char_allocator;
typedef boost::interprocess::basic_string<char, std::char_traits<char>, char_allocator> char_string;

typedef boost::interprocess::managed_unique_ptr<CatalogEntry, boost::interprocess::managed_shared_memory>::type unique_ptr_type;
typedef	boost::interprocess::deleter<CatalogEntry, segment_manager_t> deleter_type;
typedef std::pair<const idx_t, unique_ptr_type> ValueType;
typedef boost::interprocess::allocator<ValueType, segment_manager_t> ShmemAllocator;
typedef boost::unordered_map< idx_t, unique_ptr_type
			, boost::hash<idx_t>, std::equal_to<idx_t>
			, ShmemAllocator> 
EntriesUnorderedMap;

struct MappingValue {
	typedef boost::interprocess::managed_unique_ptr<MappingValue, boost::interprocess::managed_shared_memory>::type MappingValue_unique_ptr_type;
	
	explicit MappingValue(idx_t index_) : index(index_), timestamp(0), deleted(false), parent(nullptr) {
	}

	idx_t index;
	transaction_t timestamp;
	bool deleted;
	//unique_ptr<MappingValue> child;
	MappingValue_unique_ptr_type *child; // TODO deleter problem..
	MappingValue *parent;
};

struct SHM_CaseInsensitiveStringHashFunction {
	uint64_t operator()(const char_string &str) const {
		std::hash<string> hasher;
		return hasher(StringUtil::Lower(str.c_str()));
	}
};

struct SHM_CaseInsensitiveStringEquality {
	bool operator()(const char_string &a, const char_string &b) const {
		return StringUtil::Lower(a.c_str()) == StringUtil::Lower(b.c_str());
	}
};

typedef boost::interprocess::managed_unique_ptr<MappingValue, boost::interprocess::managed_shared_memory>::type MappingValue_unique_ptr_type;
typedef std::pair<const char_string, MappingValue_unique_ptr_type> map_value_type;
typedef std::pair<char_string, MappingValue_unique_ptr_type> movable_to_map_value_type;
typedef boost::interprocess::allocator<map_value_type, segment_manager_t> map_value_type_allocator;
// typedef boost::unordered_map< char_string, MappingValue_unique_ptr_type
//        	, boost::hash<char_string>, std::equal_to<std::string>, 
// 			map_value_type_allocator>
// MappingUnorderedMap;
typedef boost::unordered_map< char_string, MappingValue_unique_ptr_type
       	, SHM_CaseInsensitiveStringHashFunction, SHM_CaseInsensitiveStringEquality, 
			map_value_type_allocator>
MappingUnorderedMap;

//! The Catalog Set stores (key, value) map of a set of CatalogEntries
class CatalogSet {
	friend class DependencyManager;
	friend class EntryDropper;

public:
	DUCKDB_API explicit CatalogSet(Catalog &catalog, unique_ptr<DefaultGenerator> defaults = nullptr);
	DUCKDB_API explicit CatalogSet(Catalog &catalog, boost::interprocess::managed_shared_memory *&catalog_segment_, unique_ptr<DefaultGenerator> defaults = nullptr);

	//! Create an entry in the catalog set. Returns whether or not it was
	//! successful.
	DUCKDB_API bool CreateEntry(ClientContext &context, const string &name, unique_ptr<CatalogEntry> value,
	                            unordered_set<CatalogEntry *> &dependencies);

	DUCKDB_API bool AlterEntry(ClientContext &context, const string &name, AlterInfo *alter_info);

	DUCKDB_API bool DropEntry(ClientContext &context, const string &name, bool cascade);

	//bool AlterOwnership(ClientContext &context, ChangeOwnershipInfo *info);

	void CleanupEntry(CatalogEntry *catalog_entry);

	//! Returns the entry with the specified name
	DUCKDB_API CatalogEntry *GetEntry(ClientContext &context, const string &name);

	//! Gets the entry that is most similar to the given name (i.e. smallest levenshtein distance), or empty string if
	//! none is found. The returned pair consists of the entry name and the distance (smaller means closer).
	pair<string, idx_t> SimilarEntry(ClientContext &context, const string &name);

	//! Rollback <entry> to be the currently valid entry for a certain catalog
	//! entry
	void Undo(CatalogEntry *entry);

	//! Scan the catalog set, invoking the callback method for every committed entry
	DUCKDB_API void Scan(const std::function<void(CatalogEntry *)> &callback);
	//! Scan the catalog set, invoking the callback method for every entry
	DUCKDB_API void Scan(ClientContext &context, const std::function<void(CatalogEntry *)> &callback);

	template <class T>
	vector<T *> GetEntries(ClientContext &context) {
		vector<T *> result;
		Scan(context, [&](CatalogEntry *entry) { result.push_back((T *)entry); });
		return result;
	}

	DUCKDB_API static bool HasConflict(ClientContext &context, transaction_t timestamp);
	DUCKDB_API static bool UseTimestamp(ClientContext &context, transaction_t timestamp);

	CatalogEntry *GetEntryFromIndex(idx_t index);
	void UpdateTimestamp(CatalogEntry *entry, transaction_t timestamp);

private:
	//! Adjusts table dependencies on the event of an UNDO
	void AdjustTableDependencies(CatalogEntry *entry);
	//! Adjust one dependency
	//void AdjustDependency(CatalogEntry *entry, TableCatalogEntry *table, ColumnDefinition &column, bool remove);
	//! Adjust Enum dependency
	void AdjustEnumDependency(CatalogEntry *entry, ColumnDefinition &column, bool remove);
	//! Given a root entry, gets the entry valid for this transaction
	CatalogEntry *GetEntryForTransaction(ClientContext &context, CatalogEntry *current);
	CatalogEntry *GetCommittedEntry(CatalogEntry *current);
	bool GetEntryInternal(ClientContext &context, const string &name, idx_t &entry_index, CatalogEntry *&entry);
	bool GetEntryInternal(ClientContext &context, idx_t entry_index, CatalogEntry *&entry);
	//! Drops an entry from the catalog set; must hold the catalog_lock to safely call this
	void DropEntryInternal(ClientContext &context, idx_t entry_index, CatalogEntry &entry, bool cascade);
	CatalogEntry *CreateEntryInternal(ClientContext &context, unique_ptr<CatalogEntry> entry);
	MappingValue *GetMapping(ClientContext &context, const string &name, bool get_latest = false);
	void PutMapping(ClientContext &context, const string &name, idx_t entry_index);
	void DeleteMapping(ClientContext &context, const string &name);
	void DropEntryDependencies(ClientContext &context, idx_t entry_index, CatalogEntry &entry, bool cascade);

private:
	Catalog &catalog;
	//! The catalog lock is used to make changes to the data
	mutex catalog_lock;
	//! Mapping of string to catalog entry
	//case_insensitive_map_t<unique_ptr<MappingValue>> mapping;
	MappingUnorderedMap *mapping;
	//! The set of catalog entries
	//unordered_map<idx_t, unique_ptr<CatalogEntry>> entries;
	EntriesUnorderedMap *entries;
	//! The current catalog entry index
	idx_t *current_entry;
	//! The generator used to generate default internal entries
	unique_ptr<DefaultGenerator> defaults;
	// Shared memory manager
	boost::interprocess::managed_shared_memory *catalog_segment;
};
} // namespace duckdb
