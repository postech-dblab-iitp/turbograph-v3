#include "catalog/catalog_set.hpp"

#include "catalog/catalog.hpp"
#include "common/exception.hpp"
#include "parser/parsed_data/alter_table_info.hpp"
#include "catalog/dependency_manager.hpp"
#include "common/string_util.hpp"
#include "parser/column_definition.hpp"
#include <sys/fcntl.h>

#include <iostream>
#include "icecream.hpp"

namespace s62 {

//! Class responsible to keep track of state when removing entries from the catalog.
//! When deleting, many types of errors can be thrown, since we want to avoid try/catch blocks
//! this class makes sure that whatever elements were modified are returned to a correct state
//! when exceptions are thrown.
//! The idea here is to use RAII (Resource acquisition is initialization) to mimic a try/catch/finally block.
//! If any exception is raised when this object exists, then its destructor will be called
//! and the entry will return to its previous state during deconstruction.
class EntryDropper {
public:
	//! Both constructor and destructor are privates because they should only be called by DropEntryDependencies
	explicit EntryDropper(CatalogSet &catalog_set, idx_t entry_index)
	    : catalog_set(catalog_set), entry_index(entry_index) {
	}

	~EntryDropper() {
	}

private:
	//! The current catalog_set
	CatalogSet &catalog_set;
	//! Keeps track of the state of the entry before starting the delete
	bool old_deleted;
	//! Index of entry to be deleted
	idx_t entry_index;
};

CatalogSet::CatalogSet(Catalog &catalog, unique_ptr<DefaultGenerator> defaults)
    : catalog(&catalog), defaults(move(defaults)) {
	current_entry = new idx_t;
	*current_entry = 0;
}

CatalogSet::CatalogSet(Catalog &catalog, fixed_managed_mapped_file *&catalog_segment_, string catalog_set_name_, unique_ptr<DefaultGenerator> defaults)
    : catalog(&catalog), defaults(move(defaults)), catalog_segment(catalog_segment_) {
	string mapping_name = catalog_set_name_ + "_mapping";
	mapping = catalog_segment->find_or_construct<MappingUnorderedMap>(mapping_name.c_str())
		(3, SHM_CaseInsensitiveStringHashFunction(), SHM_CaseInsensitiveStringEquality(), 
		catalog_segment->get_allocator<map_value_type>());
	string entries_name = catalog_set_name_ + "_entries";
	entries = catalog_segment->find_or_construct<EntriesUnorderedMap>(entries_name.c_str())
		(3, boost::hash<idx_t>(), std::equal_to<idx_t>(),
		catalog_segment->get_allocator<ValueType>());
	string current_entry_name = catalog_set_name_ + "_current_entry";
	current_entry = catalog_segment->find_or_construct<idx_t>(current_entry_name.c_str())(0);
}

void CatalogSet::Load(Catalog &catalog, fixed_managed_mapped_file *&catalog_segment_, string catalog_set_name_, unique_ptr<DefaultGenerator> defaults) {
	this->catalog_segment = catalog_segment_;
	this->catalog = &catalog;
	string mapping_name = catalog_set_name_ + "_mapping";
	mapping = catalog_segment->find_or_construct<MappingUnorderedMap>(mapping_name.c_str())
		(3, SHM_CaseInsensitiveStringHashFunction(), SHM_CaseInsensitiveStringEquality(), 
		catalog_segment->get_allocator<map_value_type>());
	string entries_name = catalog_set_name_ + "_entries";
	entries = catalog_segment->find_or_construct<EntriesUnorderedMap>(entries_name.c_str())
		(3, boost::hash<idx_t>(), std::equal_to<idx_t>(),
		catalog_segment->get_allocator<ValueType>());
	string current_entry_name = catalog_set_name_ + "_current_entry";
	current_entry = catalog_segment->find_or_construct<idx_t>(current_entry_name.c_str())(0);
}

bool CatalogSet::CreateEntry(ClientContext &context, const string &name, CatalogEntry* value,
                             unordered_set<CatalogEntry *> &dependencies) {
	idx_t entry_index;
	auto mapping_value = GetMapping(context, name);
	if (mapping_value == nullptr || mapping_value->deleted) {
		// if it does not: entry has never been created

		// first create a dummy deleted entry for this entry
		// so transactions started before the commit of this transaction don't
		// see it yet
		entry_index = *current_entry;
		*current_entry = entry_index + 1;

		string dummy_name = name + "_dummy" + std::to_string(entry_index);
		auto dummy_node = catalog_segment->find_or_construct<CatalogEntry>(dummy_name.c_str())(CatalogType::INVALID, value->catalog, name, (void_allocator) catalog_segment->get_segment_manager());
		dummy_node->timestamp = 0;
		dummy_node->deleted = true;
		dummy_node->set = this;

		entries->insert_or_assign(entry_index, move(dummy_node));
		PutMapping(context, name, entry_index);
	} else {
		return true;
	}
	// create a new entry and replace the currently stored one
	// set the timestamp to the timestamp of the current transaction
	// and point it at the dummy node
	//value->timestamp = transaction.transaction_id;
	value->timestamp = 0;
	value->set = this;

	// now add the dependency set of this object to the dependency manager
	value->child = move(entries->at(entry_index));
	value->child->parent = value;
	
	entries->at(entry_index) = move(value);
	return true;
}

bool CatalogSet::GetEntryInternal(ClientContext &context, idx_t entry_index, CatalogEntry *&catalog_entry) {
	catalog_entry = entries->at(entry_index);
	// if it does: we have to retrieve the entry and to check version numbers
	if (HasConflict(context, catalog_entry->timestamp)) {
		// current version has been written to by a currently active
		// transaction
		throw TransactionException("Catalog write-write conflict on alter with \"%s\"", std::string(catalog_entry->name.data()));
	}
	// there is a current version that has been committed by this transaction
	if (catalog_entry->deleted) {
		// if the entry was already deleted, it now does not exist anymore
		// so we return that we could not find it
		return false;
	}
	return true;
}

bool CatalogSet::GetEntryInternal(ClientContext &context, const string &name, idx_t &entry_index,
                                  CatalogEntry *&catalog_entry) {
	auto mapping_value = GetMapping(context, name);
	if (mapping_value == nullptr || mapping_value->deleted) {
		// the entry does not exist, check if we can create a default entry
		return false;
	}
	entry_index = mapping_value->index;
	return GetEntryInternal(context, entry_index, catalog_entry);
}

bool CatalogSet::AlterEntry(ClientContext &context, const string &name, AlterInfo *alter_info) {
	D_ASSERT(false);
	return true;
}

void CatalogSet::DropEntryDependencies(ClientContext &context, idx_t entry_index, CatalogEntry &entry, bool cascade) {
	// Stores the deleted value of the entry before starting the process
	EntryDropper dropper(*this, entry_index);

	// To correctly delete the object and its dependencies, it temporarily is set to deleted.
	entries->at(entry_index)->deleted = true;

	// check any dependencies of this object
	entry.catalog->dependency_manager->DropObject(context, &entry, cascade);
}

void CatalogSet::DropEntryInternal(ClientContext &context, idx_t entry_index, CatalogEntry &entry, bool cascade) {
	DropEntryDependencies(context, entry_index, entry, cascade);

	// create a new entry and replace the currently stored one
	// set the timestamp to the timestamp of the current transaction
	// and point it at the dummy node
	auto value = make_unique<CatalogEntry>(CatalogType::DELETED_ENTRY, entry.catalog, entry.name, (void_allocator) catalog_segment->get_segment_manager());
	value->child->parent = value.get();
	value->set = this;
	value->deleted = true;
}

bool CatalogSet::DropEntry(ClientContext &context, const string &name, bool cascade) {
	// lock the catalog for writing
	lock_guard<mutex> write_lock(catalog->write_lock);
	// we can only delete an entry that exists
	idx_t entry_index;
	CatalogEntry *entry;
	if (!GetEntryInternal(context, name, entry_index, entry)) {
		return false;
	}
	if (entry->internal) {
		throw CatalogException("Cannot drop entry \"%s\" because it is an internal system entry", std::string(entry->name.data()));
	}

	DropEntryInternal(context, entry_index, *entry, cascade);
	return true;
}

void CatalogSet::CleanupEntry(CatalogEntry *catalog_entry) {
	// destroy the backed up entry: it is no longer required
	D_ASSERT(catalog_entry->parent);
	if (catalog_entry->parent->type != CatalogType::UPDATED_ENTRY) {
		lock_guard<mutex> lock(catalog_lock);
		if (!catalog_entry->deleted) {
			// delete the entry from the dependency manager, if it is not deleted yet
			catalog_entry->catalog->dependency_manager->EraseObject(catalog_entry);
		}
		auto parent = catalog_entry->parent;
		parent->child = move(catalog_entry->child);
	}
}

bool CatalogSet::HasConflict(ClientContext &context, transaction_t timestamp) {
	return false;
}

MappingValue *CatalogSet::GetMapping(ClientContext &context, const string &name, bool get_latest) {
	MappingValue *mapping_value;
	char_allocator temp_charallocator (catalog_segment->get_segment_manager());
	char_string name_(temp_charallocator);
	name_ = name.c_str();

	auto entry = mapping->find(name_);
	if (entry != mapping->end()) {
		mapping_value = entry->second;
	} else {
		return nullptr;
	}
	if (get_latest) {
		return mapping_value;
	}
	while (mapping_value->child) {
		if (UseTimestamp(context, mapping_value->timestamp)) {
			break;
		}
		mapping_value = mapping_value->child;
		D_ASSERT(mapping_value);
	}

	return mapping_value;
}

void CatalogSet::PutMapping(ClientContext &context, const string &name, idx_t entry_index) {
	char_allocator temp_charallocator (catalog_segment->get_segment_manager());
	char_string name_(temp_charallocator);
	name_ = name.c_str();
	auto entry = mapping->find(name_);
	string new_value_name = name + "_mapval" + std::to_string(entry_index);
	auto new_value = catalog_segment->find_or_construct<MappingValue>(new_value_name.c_str())(entry_index);
	new_value->timestamp = 0;
	if (entry != mapping->end()) {
		if (HasConflict(context, entry->second->timestamp)) {
			throw TransactionException("Catalog write-write conflict on name \"%s\"", name);
		}
		new_value->child = move(entry->second);
		new_value->child->parent = new_value;
	}
	mapping->insert(map_value_type(name_, move(new_value)));
}

void CatalogSet::DeleteMapping(ClientContext &context, const string &name) {
	D_ASSERT(false);
}

bool CatalogSet::UseTimestamp(ClientContext &context, transaction_t timestamp) {
	return true;
}

CatalogEntry *CatalogSet::GetEntryForTransaction(ClientContext &context, CatalogEntry *current) {
	while (current->child) {
		if (UseTimestamp(context, current->timestamp)) {
			break;
		}
		current = current->child.get();
		D_ASSERT(current);
	}
	return current;
}

CatalogEntry *CatalogSet::GetCommittedEntry(CatalogEntry *current) {
	return current;
}

pair<string, idx_t> CatalogSet::SimilarEntry(ClientContext &context, const string &name) {
	lock_guard<mutex> lock(catalog_lock);

	string result;
	idx_t current_score = (idx_t)-1;
	for (auto &kv : *mapping) {
		auto mapping_value = GetMapping(context, kv.first.c_str());
		if (mapping_value && !mapping_value->deleted) {
			auto ldist = StringUtil::LevenshteinDistance(kv.first.c_str(), name);
			if (ldist < current_score) {
				current_score = ldist;
				result = kv.first.c_str();
			}
		}
	}
	return {result, current_score};
}

CatalogEntry *CatalogSet::CreateEntryInternal(ClientContext &context, unique_ptr<CatalogEntry> entry) {
	auto &name = entry->name;
	auto entry_index = *current_entry;
	*current_entry = entry_index + 1;
	auto catalog_entry = entry.get();

	entry->timestamp = 0;

	PutMapping(context, std::string(name.data()), entry_index);
	return catalog_entry;
}

CatalogEntry *CatalogSet::GetEntry(ClientContext &context, const string &name) {
	auto mapping_value = GetMapping(context, name);
	if (mapping_value != nullptr && !mapping_value->deleted) {
		// we found an entry for this name
		// check the version numbers

		auto catalog_entry = entries->at(mapping_value->index);
		CatalogEntry *current = GetEntryForTransaction(context, catalog_entry);
		if (current->deleted || (std::string(current->name.data()) != name && !UseTimestamp(context, mapping_value->timestamp))) {
			return nullptr;
		}
		return current;
	}
icecream::ic.enable(); IC(name); icecream::ic.disable();
	// no entry found with this name, check for defaults
	if (!defaults || defaults->created_all_entries) {
		// no defaults either: return null
		icecream::ic.enable(); IC(); icecream::ic.disable();
		return nullptr;
	}

	// this catalog set has a default map defined
	// check if there is a default entry that we can create with this name
	auto entry = defaults->CreateDefaultEntry(context, name);
	if (!entry) {
		// no default entry
		return nullptr;
	}
	// there is a default entry! create it
	auto result = CreateEntryInternal(context, move(entry));
	if (result) {
		return result;
	}
	return GetEntry(context, name);
}

void CatalogSet::UpdateTimestamp(CatalogEntry *entry, transaction_t timestamp) {
	entry->timestamp = timestamp;
}

void CatalogSet::AdjustEnumDependency(CatalogEntry *entry, ColumnDefinition &column, bool remove) {
	D_ASSERT(false);
}

void CatalogSet::AdjustTableDependencies(CatalogEntry *entry) {
	D_ASSERT(false);
}

void CatalogSet::Undo(CatalogEntry *entry) {
	D_ASSERT(false);
}

void CatalogSet::Scan(ClientContext &context, const std::function<void(CatalogEntry *)> &callback) {
	// lock the catalog set
	unique_lock<mutex> lock(catalog_lock);
	if (defaults && !defaults->created_all_entries) {
		// this catalog set has a default set defined:
		auto default_entries = defaults->GetDefaultEntries();
		char_allocator temp_charallocator (catalog_segment->get_segment_manager());
		char_string default_entry_(temp_charallocator);
		for (auto &default_entry : default_entries) {
			default_entry_ = default_entry.c_str();
			auto map_entry = mapping->find(default_entry_);
			if (map_entry == mapping->end()) {
				// we unlock during the CreateEntry, since it might reference other catalog sets...
				// specifically for views this can happen since the view will be bound
				lock.unlock();
				auto entry = defaults->CreateDefaultEntry(context, default_entry);

				lock.lock();
				CreateEntryInternal(context, move(entry));
			}
		}
		defaults->created_all_entries = true;
	}
	for (auto &kv : *entries) {
		auto entry = kv.second;
		entry = GetEntryForTransaction(context, entry);
		if (!entry->deleted) {
			callback(entry);
		}
	}
}

void CatalogSet::Scan(const std::function<void(CatalogEntry *)> &callback) {
	// lock the catalog set
	lock_guard<mutex> lock(catalog_lock);
	for (auto &kv : *entries) {
		auto entry = kv.second;
		entry = GetCommittedEntry(entry);
		if (!entry->deleted) {
			callback(entry);
		}
	}
}
} // namespace s62
