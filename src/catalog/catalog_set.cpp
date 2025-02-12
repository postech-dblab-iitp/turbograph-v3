#include "catalog/catalog_set.hpp"
#include "catalog/dependency_manager.hpp"
#include "catalog/catalog.hpp"

#include "common/exception.hpp"
#include "parser/parsed_data/alter_table_info.hpp"
#include "common/string_util.hpp"
#include "parser/column_definition.hpp"
#include "main/client_context.hpp"
#include <sys/fcntl.h>

#include <iostream>
#include "icecream.hpp"

namespace duckdb {

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
		//old_deleted = (*catalog_set.entries)[entry_index].get()->deleted;
	}

	~EntryDropper() {
		//(*catalog_set.entries)[entry_index].get()->deleted = old_deleted;
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
    : catalog(&catalog), defaults(move(defaults)) {
	string mapping_name = catalog_set_name_ + "_mapping";
	mapping = catalog_segment_->find_or_construct<MappingUnorderedMap>(mapping_name.c_str())
		(3, SHM_CaseInsensitiveStringHashFunction(), SHM_CaseInsensitiveStringEquality(), 
		catalog_segment_->get_allocator<map_value_type>());
	string entries_name = catalog_set_name_ + "_entries";
	entries = catalog_segment_->find_or_construct<EntriesUnorderedMap>(entries_name.c_str())
		(3, boost::hash<idx_t>(), std::equal_to<idx_t>(),
		catalog_segment_->get_allocator<ValueType>());
	string current_entry_name = catalog_set_name_ + "_current_entry";
	current_entry = catalog_segment_->find_or_construct<idx_t>(current_entry_name.c_str())(0);
}

void CatalogSet::Load(Catalog &catalog, fixed_managed_mapped_file *&catalog_segment_, string catalog_set_name_, unique_ptr<DefaultGenerator> defaults) {
	this->catalog = &catalog;
	string mapping_name = catalog_set_name_ + "_mapping";
	mapping = catalog_segment_->find_or_construct<MappingUnorderedMap>(mapping_name.c_str())
		(3, SHM_CaseInsensitiveStringHashFunction(), SHM_CaseInsensitiveStringEquality(), 
		catalog_segment_->get_allocator<map_value_type>());
	string entries_name = catalog_set_name_ + "_entries";
	entries = catalog_segment_->find_or_construct<EntriesUnorderedMap>(entries_name.c_str())
		(3, boost::hash<idx_t>(), std::equal_to<idx_t>(),
		catalog_segment_->get_allocator<ValueType>());
	string current_entry_name = catalog_set_name_ + "_current_entry";
	current_entry = catalog_segment_->find_or_construct<idx_t>(current_entry_name.c_str())(0);
}

/*bool CatalogSet::CreateEntry(ClientContext &context, const string &name, unique_ptr<CatalogEntry> value,
                             unordered_set<CatalogEntry *> &dependencies) {
	//auto &transaction = Transaction::GetTransaction(context);
	// lock the catalog for writing
	lock_guard<mutex> write_lock(catalog.write_lock);
	// lock this catalog set to disallow reading
	lock_guard<mutex> read_lock(catalog_lock);

	// first check if the entry exists in the unordered set
	idx_t entry_index;
	auto mapping_value = GetMapping(context, name);
	if (mapping_value == nullptr || mapping_value->deleted) {
		// if it does not: entry has never been created

		// first create a dummy deleted entry for this entry
		// so transactions started before the commit of this transaction don't
		// see it yet
		entry_index = *current_entry;
		*current_entry = entry_index + 1;

		string dummy_name = "dummy" + std::to_string(entry_index);
		ValueType dummy_node = ValueType(entry_index, 
			boost::interprocess::make_managed_unique_ptr(catalog_segment->construct<CatalogEntry>(dummy_name.c_str())(CatalogType::INVALID, value->catalog, name), *catalog_segment).get());
		dummy_node.second->timestamp = 0;
		dummy_node.second->deleted = true;
		dummy_node.second->set = this;

		entries->insert_or_assign(entry_index, move(dummy_node.second));
		PutMapping(context, name, entry_index);
	} else {
		entry_index = mapping_value->index;
		auto &current = *entries->at(entry_index);
		// if it does, we have to check version numbers
		if (HasConflict(context, current.timestamp)) {
			// current version has been written to by a currently active
			// transaction
			throw TransactionException("Catalog write-write conflict on create with \"%s\"", current.name);
		}
		// there is a current version that has been committed
		// if it has not been deleted there is a conflict
		if (!current.deleted) {
			return false;
		}
	}
	// create a new entry and replace the currently stored one
	// set the timestamp to the timestamp of the current transaction
	// and point it at the dummy node
	//value->timestamp = transaction.transaction_id;
	value->timestamp = 0;
	value->set = this;

	// now add the dependency set of this object to the dependency manager
	catalog.dependency_manager->AddObject(context, value.get(), dependencies);

	//value->child = move(entries[entry_index]); //TODO
	value->child->parent = value.get();
	// push the old entry in the undo buffer for this transaction
	//transaction.PushCatalogEntry(value->child.get());
	//entries[entry_index] = move(value); //TODO
	D_ASSERT(false);
	return true;
}*/

bool CatalogSet::CreateEntry(ClientContext &context, const string &name, CatalogEntry* value,
                             unordered_set<CatalogEntry *> &dependencies) {
	//auto &transaction = Transaction::GetTransaction(context);
	// lock the catalog for writing
	// lock_guard<mutex> write_lock(catalog->write_lock); // TODO 240103 tslee disable lock not work
	// lock this catalog set to disallow reading
	// lock_guard<mutex> read_lock(catalog_lock); // TODO 240103 tslee disable lock not work

	// first check if the entry exists in the unordered set
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
        auto dummy_node =
            context.GetCatalogSHM()->find_or_construct<CatalogEntry>(
                dummy_name.c_str())(
                CatalogType::INVALID, value->catalog, name,
                (void_allocator)context.GetCatalogSHM()->get_segment_manager());
        //ValueType dummy_node = ValueType(entry_index, 
		//	boost::interprocess::make_managed_unique_ptr(catalog_segment->construct<CatalogEntry>(dummy_name.c_str())(CatalogType::INVALID, value->catalog, name), *catalog_segment).get());
		dummy_node->timestamp = 0;
		dummy_node->deleted = true;
		dummy_node->set = this;

		entries->insert_or_assign(entry_index, move(dummy_node));
		PutMapping(context, name, entry_index);
	} else {
		return true; // XXX In the current single thread version, we just return (LoadCatalog Case)
		// entry_index = mapping_value->index;
		// auto &current = *entries->at(entry_index);
		// // if it does, we have to check version numbers
		// if (HasConflict(context, current.timestamp)) {
		// 	// current version has been written to by a currently active
		// 	// transaction
		// 	throw TransactionException("Catalog write-write conflict on create with \"%s\"", std::string(current.name.data()));
		// }
		// // there is a current version that has been committed
		// // if it has not been deleted there is a conflict
		// if (!current.deleted) {
		// 	return false;
		// }
	}
	// create a new entry and replace the currently stored one
	// set the timestamp to the timestamp of the current transaction
	// and point it at the dummy node
	//value->timestamp = transaction.transaction_id;
	value->timestamp = 0;
	value->set = this;

	// now add the dependency set of this object to the dependency manager
	// catalog->dependency_manager->AddObject(context, value, dependencies); // TODO 240103 tslee disabled this.. error occur

	value->child = move(entries->at(entry_index));
	value->child->parent = value;
	// push the old entry in the undo buffer for this transaction
	//transaction.PushCatalogEntry(value->child.get());
	entries->at(entry_index) = move(value);
	return true;
}

/*bool CatalogSet::CreateEntry(ClientContext &context, const string &name, boost::interprocess::offset_ptr<CatalogEntry> value,
                             unordered_set<CatalogEntry *> &dependencies) {
	//auto &transaction = Transaction::GetTransaction(context);
	// lock the catalog for writing
	lock_guard<mutex> write_lock(catalog.write_lock);
	// lock this catalog set to disallow reading
	lock_guard<mutex> read_lock(catalog_lock);

	// first check if the entry exists in the unordered set
	idx_t entry_index;
	auto mapping_value = GetMapping(context, name);
	if (mapping_value == nullptr || mapping_value->deleted) {
		// if it does not: entry has never been created

		// first create a dummy deleted entry for this entry
		// so transactions started before the commit of this transaction don't
		// see it yet
		entry_index = *current_entry;
		*current_entry = entry_index + 1;

		string dummy_name = "dummy" + std::to_string(entry_index);
		ValueType dummy_node = ValueType(entry_index, 
			boost::interprocess::make_managed_unique_ptr(catalog_segment->construct<CatalogEntry>(dummy_name.c_str())(CatalogType::INVALID, value->catalog, name), *catalog_segment).get());
		dummy_node.second->timestamp = 0;
		dummy_node.second->deleted = true;
		dummy_node.second->set = this;

		entries->insert_or_assign(entry_index, move(dummy_node.second));
		PutMapping(context, name, entry_index);
	} else {
		entry_index = mapping_value->index;
		auto &current = *entries->at(entry_index);
		// if it does, we have to check version numbers
		if (HasConflict(context, current.timestamp)) {
			// current version has been written to by a currently active
			// transaction
			throw TransactionException("Catalog write-write conflict on create with \"%s\"", current.name);
		}
		// there is a current version that has been committed
		// if it has not been deleted there is a conflict
		if (!current.deleted) {
			return false;
		}
	}
	// create a new entry and replace the currently stored one
	// set the timestamp to the timestamp of the current transaction
	// and point it at the dummy node
	//value->timestamp = transaction.transaction_id;
	value->timestamp = 0;
	value->set = this;

	// now add the dependency set of this object to the dependency manager
	catalog.dependency_manager->AddObject(context, value.get(), dependencies);

	//value->child = move(entries[entry_index]); //TODO
	value->child = move(entries->at(entry_index).get());
	value->child->parent = value.get();
	// push the old entry in the undo buffer for this transaction
	//transaction.PushCatalogEntry(value->child.get());
	//entries[entry_index] = move(value); //TODO
	entries->insert_or_assign(entry_index, boost::move(value));
	return true;
}*/

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

/*bool CatalogSet::AlterOwnership(ClientContext &context, ChangeOwnershipInfo *info) {
	idx_t entry_index;
	CatalogEntry *entry;
	if (!GetEntryInternal(context, info->name, entry_index, entry)) {
		return false;
	}

	auto owner_entry = catalog.GetEntry(context, info->owner_schema, info->owner_name);
	if (!owner_entry) {
		return false;
	}

	catalog.dependency_manager->AddOwnership(context, owner_entry, entry);

	return true;
}*/

bool CatalogSet::AlterEntry(ClientContext &context, const string &name, AlterInfo *alter_info) {
	D_ASSERT(false);
	return true;
	/*auto &transaction = Transaction::GetTransaction(context);
	// lock the catalog for writing
	lock_guard<mutex> write_lock(catalog.write_lock);

	// first check if the entry exists in the unordered set
	idx_t entry_index;
	CatalogEntry *entry;
	if (!GetEntryInternal(context, name, entry_index, entry)) {
		return false;
	}
	if (entry->internal) {
		throw CatalogException("Cannot alter entry \"%s\" because it is an internal system entry", entry->name);
	}

	// lock this catalog set to disallow reading
	lock_guard<mutex> read_lock(catalog_lock);

	// create a new entry and replace the currently stored one
	// set the timestamp to the timestamp of the current transaction
	// and point it to the updated table node
	string original_name = entry->name;
	auto value = entry->AlterEntry(context, alter_info);
	if (!value) {
		// alter failed, but did not result in an error
		return true;
	}

	if (value->name != original_name) {
		auto mapping_value = GetMapping(context, value->name);
		if (mapping_value && !mapping_value->deleted) {
			auto entry = GetEntryForTransaction(context, entries[mapping_value->index].get());
			if (!entry->deleted) {
				string rename_err_msg =
				    "Could not rename \"%s\" to \"%s\": another entry with this name already exists!";
				throw CatalogException(rename_err_msg, original_name, value->name);
			}
		}
		PutMapping(context, value->name, entry_index);
		DeleteMapping(context, original_name);
	}
	//! Check the dependency manager to verify that there are no conflicting dependencies with this alter
	catalog.dependency_manager->AlterObject(context, entry, value.get());

	value->timestamp = transaction.transaction_id;
	value->child = move(entries[entry_index]);
	value->child->parent = value.get();
	value->set = this;

	// serialize the AlterInfo into a temporary buffer
	BufferedSerializer serializer;
	alter_info->Serialize(serializer);
	BinaryData serialized_alter = serializer.GetData();

	// push the old entry in the undo buffer for this transaction
	transaction.PushCatalogEntry(value->child.get(), serialized_alter.data.get(), serialized_alter.size);
	entries[entry_index] = move(value);

	return true;*/
}

void CatalogSet::DropEntryDependencies(ClientContext &context, idx_t entry_index, CatalogEntry &entry, bool cascade) {

	// Stores the deleted value of the entry before starting the process
	EntryDropper dropper(*this, entry_index);

	// To correctly delete the object and its dependencies, it temporarily is set to deleted.
	entries->at(entry_index)->deleted = true;

	// check any dependencies of this object
	entry.catalog->dependency_manager->DropObject(context, &entry, cascade);

	// dropper destructor is called here
	// the destructor makes sure to return the value to the previous state
	// dropper.~EntryDropper()
}

void CatalogSet::DropEntryInternal(ClientContext &context, idx_t entry_index, CatalogEntry &entry, bool cascade) {
	//auto &transaction = Transaction::GetTransaction(context);

	DropEntryDependencies(context, entry_index, entry, cascade);

	// create a new entry and replace the currently stored one
	// set the timestamp to the timestamp of the current transaction
	// and point it at the dummy node
	auto value = make_unique<CatalogEntry>(CatalogType::DELETED_ENTRY, entry.catalog, entry.name, (void_allocator) context.GetCatalogSHM()->get_segment_manager());
	//value->timestamp = transaction.transaction_id;
	//value->child = move(entries[entry_index]); //TODO
	value->child->parent = value.get();
	value->set = this;
	value->deleted = true;

	// push the old entry in the undo buffer for this transaction
	//transaction.PushCatalogEntry(value->child.get());

	//entries[entry_index] = move(value); //TODO
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
		if (parent->deleted && !parent->child && !parent->parent) {
			/*auto mapping_entry = mapping->find(parent->name.c_str());
			D_ASSERT(mapping_entry != mapping->end());
			auto index = mapping_entry->second->index;
			auto entry = entries->find(index);
			D_ASSERT(entry != entries->end());
			if (entry->second.get() == parent) {
				mapping->erase(mapping_entry);
				entries->erase(entry);
			}*/
		}
	}
}

bool CatalogSet::HasConflict(ClientContext &context, transaction_t timestamp) {
	return false;
	//auto &transaction = Transaction::GetTransaction(context);
	//return (timestamp >= TRANSACTION_ID_START && timestamp != transaction.transaction_id) ||
	//       (timestamp < TRANSACTION_ID_START && timestamp > transaction.start_time);
}

MappingValue *CatalogSet::GetMapping(ClientContext &context, const string &name, bool get_latest) {
	MappingValue *mapping_value;
	char_allocator temp_charallocator (context.GetCatalogSHM()->get_segment_manager());
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
	char_allocator temp_charallocator (context.GetCatalogSHM()->get_segment_manager());
	char_string name_(temp_charallocator);
	name_ = name.c_str();
	auto entry = mapping->find(name_);
	string new_value_name = name + "_mapval" + std::to_string(entry_index);
	auto new_value = context.GetCatalogSHM()->find_or_construct<MappingValue>(new_value_name.c_str())(entry_index);
	//auto new_value = make_unique<MappingValue>(entry_index);
	//new_value->timestamp = Transaction::GetTransaction(context).transaction_id;
	new_value->timestamp = 0;
	if (entry != mapping->end()) {
		if (HasConflict(context, entry->second->timestamp)) {
			throw TransactionException("Catalog write-write conflict on name \"%s\"", name);
		}
		new_value->child = move(entry->second);
		new_value->child->parent = new_value;
	}
	mapping->insert(map_value_type(name_, move(new_value)));
	//mapping->insert_or_assign(name.c_str(), move(new_value));
	//mapping[name] = move(new_value);
}

void CatalogSet::DeleteMapping(ClientContext &context, const string &name) {
	D_ASSERT(false);
	/*auto entry = mapping->find(name.c_str());
	D_ASSERT(entry != mapping->end());
	auto delete_marker = make_unique<MappingValue>(entry->second->index);
	delete_marker->deleted = true;
	//delete_marker->timestamp = Transaction::GetTransaction(context).transaction_id;
	delete_marker->timestamp = 0;
	delete_marker->child = move(entry->second);
	delete_marker->child->parent = delete_marker.get();
	//mapping->at(name.c_str()) = move(delete_marker);
	mapping[name] = move(delete_marker);*/
}

bool CatalogSet::UseTimestamp(ClientContext &context, transaction_t timestamp) {
	return true;
	/*auto &transaction = Transaction::GetTransaction(context);
	if (timestamp == transaction.transaction_id) {
		// we created this version
		return true;
	}
	if (timestamp < transaction.start_time) {
		// this version was commited before we started the transaction
		return true;
	}
	return false;*/
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
	// while (current->child) {
	// 	break;
	// 	//if (current->timestamp < TRANSACTION_ID_START) {
	// 	//	// this entry is committed: use it
	// 	//	break;
	// 	//}
	// 	current = current->child.get();
	// 	D_ASSERT(current);
	// }
	// return current;
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
	/*if (mapping->find(entry->name.c_str()) != mapping->end()) {
		return nullptr;
	}*/
	auto &name = entry->name;
	auto entry_index = *current_entry;
	*current_entry = entry_index + 1;
	auto catalog_entry = entry.get();

	entry->timestamp = 0;

	PutMapping(context, std::string(name.data()), entry_index);
	//mapping->at(name.c_str())->timestamp = 0;
	//mapping[name]->timestamp = 0;
	//entries[entry_index] = move(entry); //TODO
	return catalog_entry;
}

CatalogEntry *CatalogSet::GetEntry(ClientContext &context, const string &name) {
	// unique_lock<mutex> lock(catalog_lock); // TODO disable in debug phase..
	// fprintf(stdout, "CatalogSet address %p\n", this);
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

	// no entry found with this name, check for defaults
	if (!defaults || defaults->created_all_entries) {
		// no defaults either: return null
		icecream::ic.enable(); IC(); icecream::ic.disable();
		return nullptr;
	}

	// this catalog set has a default map defined
	// check if there is a default entry that we can create with this name
	// lock.unlock(); // XXX disable in debug phase..
	auto entry = defaults->CreateDefaultEntry(context, name);
	// lock.lock(); // XXX disable in debug phase
	if (!entry) {
		// no default entry
		return nullptr;
	}
	// there is a default entry! create it
	auto result = CreateEntryInternal(context, move(entry));
	if (result) {
		return result;
	}
	// we found a default entry, but failed
	// this means somebody else created the entry first
	// just retry?
	// lock.unlock(); // XXX disable in debug phase..
	return GetEntry(context, name);
}

void CatalogSet::UpdateTimestamp(CatalogEntry *entry, transaction_t timestamp) {
	entry->timestamp = timestamp;
	//mapping->at(entry->name.c_str())->timestamp = timestamp;
	//mapping[entry->name]->timestamp = timestamp;
}

void CatalogSet::AdjustEnumDependency(CatalogEntry *entry, ColumnDefinition &column, bool remove) {
	D_ASSERT(false);
	/*CatalogEntry *enum_type_catalog = (CatalogEntry *)EnumType::GetCatalog(column.type);
	if (enum_type_catalog) {
		if (remove) {
			catalog.dependency_manager->dependents_map[enum_type_catalog].erase(entry->parent);
			catalog.dependency_manager->dependencies_map[entry->parent].erase(enum_type_catalog);
		} else {
			catalog.dependency_manager->dependents_map[enum_type_catalog].insert(entry);
			catalog.dependency_manager->dependencies_map[entry].insert(enum_type_catalog);
		}
	}*/
}

/*void CatalogSet::AdjustDependency(CatalogEntry *entry, TableCatalogEntry *table, ColumnDefinition &column,
                                  bool remove) {
	bool found = false;
	if (column.type.id() == LogicalTypeId::ENUM) {
		for (auto &old_column : table->columns) {
			if (old_column.name == column.name && old_column.type.id() != LogicalTypeId::ENUM) {
				AdjustEnumDependency(entry, column, remove);
				found = true;
			}
		}
		if (!found) {
			AdjustEnumDependency(entry, column, remove);
		}
	}
}*/

void CatalogSet::AdjustTableDependencies(CatalogEntry *entry) {
	D_ASSERT(false);
	/*if (entry->type == CatalogType::TABLE_ENTRY && entry->parent->type == CatalogType::TABLE_ENTRY) {
		// If it's a table entry we have to check for possibly removing or adding user type dependencies
		auto old_table = (TableCatalogEntry *)entry->parent;
		auto new_table = (TableCatalogEntry *)entry;

		for (auto &new_column : new_table->columns) {
			AdjustDependency(entry, old_table, new_column, false);
		}
		for (auto &old_column : old_table->columns) {
			AdjustDependency(entry, new_table, old_column, true);
		}
	}*/
}

void CatalogSet::Undo(CatalogEntry *entry) {
	D_ASSERT(false);
	/*
	lock_guard<mutex> write_lock(catalog.write_lock);

	lock_guard<mutex> lock(catalog_lock);

	// entry has to be restored
	// and entry->parent has to be removed ("rolled back")

	// i.e. we have to place (entry) as (entry->parent) again
	auto &to_be_removed_node = entry->parent;

	AdjustTableDependencies(entry);

	if (!to_be_removed_node->deleted) {
		// delete the entry from the dependency manager as well
		catalog.dependency_manager->EraseObject(to_be_removed_node);
	}
	if (entry->name != to_be_removed_node->name) {
		// rename: clean up the new name when the rename is rolled back
		auto removed_entry = mapping->find(to_be_removed_node->name.c_str());
		if (removed_entry->second->child) {
			removed_entry->second->child->parent = nullptr;
			mapping[to_be_removed_node->name] = move(removed_entry->second->child);
		} else {
			mapping->erase(removed_entry);
		}
	}
	if (to_be_removed_node->parent) {
		// if the to be removed node has a parent, set the child pointer to the
		// to be restored node
		to_be_removed_node->parent->child = move(to_be_removed_node->child);
		entry->parent = to_be_removed_node->parent;
	} else {
		// otherwise we need to update the base entry tables
		auto &name = entry->name;
		to_be_removed_node->child->SetAsRoot();
		//entries[mapping[name]->index] = move(to_be_removed_node->child); //TODO
		entry->parent = nullptr;
	}

	// restore the name if it was deleted
	auto restored_entry = mapping->find(entry->name.c_str());
	if (restored_entry->second->deleted || entry->type == CatalogType::INVALID) {
		if (restored_entry->second->child) {
			restored_entry->second->child->parent = nullptr;
			mapping[entry->name] = move(restored_entry->second->child);
		} else {
			mapping->erase(restored_entry);
		}
	}
	// we mark the catalog as being modified, since this action can lead to e.g. tables being dropped
	entry->catalog->ModifyCatalog();
	*/
}

void CatalogSet::Scan(ClientContext &context, const std::function<void(CatalogEntry *)> &callback) {
	// lock the catalog set
	unique_lock<mutex> lock(catalog_lock);
	if (defaults && !defaults->created_all_entries) {
		// this catalog set has a default set defined:
		auto default_entries = defaults->GetDefaultEntries();
		char_allocator temp_charallocator (context.GetCatalogSHM()->get_segment_manager());
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

CatalogSetInMem::CatalogSetInMem(Catalog &catalog, unique_ptr<DefaultGenerator> defaults)
    : CatalogSet(catalog, move(defaults)) {
	mapping = make_unique<case_insensitive_map_t<MappingValue *>>();
	entries = make_unique<unordered_map<idx_t, CatalogEntry *>>();
}

CatalogSetInMem::~CatalogSetInMem() {
	for (auto &kv : *entries) {
		delete kv.second;
	}

	for (auto &kv : *mapping) {
		delete kv.second;
	}
}

bool CatalogSetInMem::CreateEntry(ClientContext &context, const string &name, CatalogEntry* value,
                             unordered_set<CatalogEntry *> &dependencies) {
	// first check if the entry exists in the unordered set
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
        auto dummy_node =
			new CatalogEntry(CatalogType::INVALID, value->catalog, name, 
			(void_allocator)context.GetCatalogSHM()->get_segment_manager());
        
		dummy_node->timestamp = 0;
		dummy_node->deleted = true;
		dummy_node->set = this;

		auto it = entries->find(entry_index);
		if (it != entries->end()) delete it->second;
		entries->insert_or_assign(entry_index, move(dummy_node));
		PutMapping(context, name, entry_index);
	} else {
		return true; // XXX In the current single thread version, we just return (LoadCatalog Case)
	}
	// create a new entry and replace the currently stored one
	// set the timestamp to the timestamp of the current transaction
	// and point it at the dummy node
	//value->timestamp = transaction.transaction_id;
	value->timestamp = 0;
	value->set = this;

	// now add the dependency set of this object to the dependency manager
	// catalog->dependency_manager->AddObject(context, value, dependencies); // TODO 240103 tslee disabled this.. error occur

	// value->child = move(entries->at(entry_index));
	// value->child->parent = value;
	// push the old entry in the undo buffer for this transaction
	//transaction.PushCatalogEntry(value->child.get());
	auto it = entries->find(entry_index);
	if (it != entries->end()) delete it->second;
	entries->insert_or_assign(entry_index, move(value));
	return true;
}

void CatalogSetInMem::PutMapping(ClientContext &context, const string &name, idx_t entry_index) {
	auto entry = mapping->find(name);
	string new_value_name = name + "_mapval" + std::to_string(entry_index);

	auto new_value = new MappingValue(entry_index);
	//new_value->timestamp = Transaction::GetTransaction(context).transaction_id;
	new_value->timestamp = 0;
	if (entry != mapping->end()) {
		if (HasConflict(context, entry->second->timestamp)) {
			throw TransactionException("Catalog write-write conflict on name \"%s\"", name);
		}
		new_value->child = move(entry->second);
		new_value->child->parent = new_value;
	}
	mapping->insert(std::make_pair(name, move(new_value)));
	//mapping->insert_or_assign(name.c_str(), move(new_value));
	// mapping[name] = move(new_value);
}

MappingValue *CatalogSetInMem::GetMapping(ClientContext &context, const string &name, bool get_latest) {
	MappingValue *mapping_value;

	auto entry = mapping->find(name);
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

CatalogEntry *CatalogSetInMem::GetEntry(ClientContext &context, const string &name) {
	// unique_lock<mutex> lock(catalog_lock); // TODO disable in debug phase..
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

	return nullptr;
}

} // namespace duckdb
