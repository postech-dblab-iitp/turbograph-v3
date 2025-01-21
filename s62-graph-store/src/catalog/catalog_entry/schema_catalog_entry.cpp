#include "catalog/catalog_entry/schema_catalog_entry.hpp"

#include "catalog/catalog.hpp"
#include "catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "catalog/catalog_entry/index_catalog_entry.hpp"
#include "catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "catalog/catalog_entry/graph_catalog_entry.hpp"
#include "catalog/catalog_entry/partition_catalog_entry.hpp"
#include "catalog/catalog_entry/property_schema_catalog_entry.hpp"
#include "catalog/catalog_entry/extent_catalog_entry.hpp"
#include "catalog/catalog_entry/chunkdefinition_catalog_entry.hpp"
#include "common/exception.hpp"
#include "parser/parsed_data/alter_table_info.hpp"
#include "parser/parsed_data/create_graph_info.hpp"
#include "parser/parsed_data/create_partition_info.hpp"
#include "parser/parsed_data/create_property_schema_info.hpp"
#include "parser/parsed_data/create_extent_info.hpp"
#include "parser/parsed_data/create_chunkdefinition_info.hpp"
#include "parser/parsed_data/create_index_info.hpp"
#include "parser/parsed_data/create_scalar_function_info.hpp"
#include "parser/parsed_data/create_schema_info.hpp"
#include "parser/parsed_data/drop_info.hpp"

#include <algorithm>
#include <sstream>
#include <iostream>
#include "icecream.hpp"

namespace s62 {

SchemaCatalogEntry::SchemaCatalogEntry(Catalog *catalog, string name_p, bool internal, fixed_managed_mapped_file *&catalog_segment)
    : CatalogEntry(CatalogType::SCHEMA_ENTRY, catalog, move(name_p), (void_allocator) catalog_segment->get_segment_manager()), 
		graphs(*catalog, catalog_segment, std::string(this->name.data()) + std::string("_graphs")),
		partitions(*catalog, catalog_segment, std::string(this->name.data()) + std::string("_partitions")),
		propertyschemas(*catalog, catalog_segment, std::string(this->name.data()) + std::string("_propertyschemas")), 
		extents(*catalog, catalog_segment, std::string(this->name.data()) + std::string("_extents")), 
		chunkdefinitions(*catalog, catalog_segment, std::string(this->name.data()) + std::string("_chunkdefinitions")),
		indexes(*catalog, catalog_segment, std::string(this->name.data()) + std::string("_indexes")),
		functions(*catalog, catalog_segment, std::string(this->name.data()) + std::string("_functions")),
		oid_to_catalog_entry_array((void_allocator) catalog_segment->get_segment_manager()) {
	this->internal = internal;
	this->catalog_segment = catalog_segment;
}

CatalogEntry *SchemaCatalogEntry::AddEntry(ClientContext &context, StandardEntry *entry,
                                           OnCreateConflict on_conflict, unordered_set<CatalogEntry *> dependencies) {
	auto entry_name = entry->name;
	auto entry_type = entry->type;
	auto result = entry;

	// first find the set for this entry
	auto &set = GetCatalogSet(entry_type);

	if (name != TEMP_SCHEMA) {
		dependencies.insert(this);
	} else {
		entry->temporary = true;
	}
	if (on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		// CREATE OR REPLACE: first try to drop the entry
		auto old_entry = set.GetEntry(context, std::string(entry_name.data()));
		if (old_entry) {
			if (old_entry->type != entry_type) {
				throw CatalogException("Existing object %s is of type %s, trying to replace with type %s", std::string(entry_name.data()),
				                       CatalogTypeToString(old_entry->type), CatalogTypeToString(entry_type));
			}
			(void)set.DropEntry(context, std::string(entry_name.data()), false);
		}
	}
	// now try to add the entry
	if (!set.CreateEntry(context, std::string(entry_name.data()), move(entry), dependencies)) {
		// entry already exists!
		if (on_conflict == OnCreateConflict::ERROR_ON_CONFLICT) {
			throw CatalogException("%s with name \"%s\" already exists!", CatalogTypeToString(entry_type), std::string(entry_name.data()));
		} else {
			return nullptr;
		}
	}
	oid_to_catalog_entry_array.insert({result->GetOid(), (void *)result});
	return result;
}

CatalogEntry *SchemaCatalogEntry::AddEntry(ClientContext &context, StandardEntry *entry,
                                           OnCreateConflict on_conflict) {
	unordered_set<CatalogEntry *> dependencies;
	return AddEntry(context, move(entry), on_conflict, dependencies);
}

bool SchemaCatalogEntry::AddEntryInternal(ClientContext &context, CatalogEntry *entry, string &entry_name, CatalogType &entry_type,
                                           OnCreateConflict on_conflict, unordered_set<CatalogEntry *> dependencies) {
											   // first find the set for this entry
	auto &set = GetCatalogSet(entry_type);

	if (name != TEMP_SCHEMA) {
		dependencies.insert(this);
	} else {
		entry->temporary = true;
	}
	if (on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		// CREATE OR REPLACE: first try to drop the entry
		auto old_entry = set.GetEntry(context, entry_name);
		if (old_entry) {
			if (old_entry->type != entry_type) {
				throw CatalogException("Existing object %s is of type %s, trying to replace with type %s", entry_name,
				                       CatalogTypeToString(old_entry->type), CatalogTypeToString(entry_type));
			}
			(void)set.DropEntry(context, entry_name, false);
		}
	}
	
	// now try to add the entry
	if (!set.CreateEntry(context, entry_name, move(entry), dependencies)) {
		// entry already exists!
		if (on_conflict == OnCreateConflict::ERROR_ON_CONFLICT) {
			throw CatalogException("%s with name \"%s\" already exists!", CatalogTypeToString(entry_type), entry_name);
		} else {
			return false;
		}
	}
	return true;
}

CatalogEntry *SchemaCatalogEntry::CreateGraph(ClientContext &context, CreateGraphInfo *info) {
	unordered_set<CatalogEntry *> dependencies;
	void_allocator alloc_inst (catalog_segment->get_segment_manager());
	auto graph = catalog_segment->find_or_construct<GraphCatalogEntry>(info->graph.c_str())(catalog, this, info, alloc_inst);
	return AddEntry(context, move(graph), info->on_conflict, dependencies);
}

CatalogEntry *SchemaCatalogEntry::CreatePartition(ClientContext &context, CreatePartitionInfo *info) {
	unordered_set<CatalogEntry *> dependencies;
	void_allocator alloc_inst (catalog_segment->get_segment_manager());
	auto partition = catalog_segment->find_or_construct<PartitionCatalogEntry>(info->partition.c_str())(catalog, this, info, alloc_inst);
	return AddEntry(context, move(partition), info->on_conflict, dependencies);
}

CatalogEntry *SchemaCatalogEntry::CreatePropertySchema(ClientContext &context, CreatePropertySchemaInfo *info) {
	unordered_set<CatalogEntry *> dependencies;
	void_allocator alloc_inst (catalog_segment->get_segment_manager());
	auto propertyschema = catalog_segment->find_or_construct<PropertySchemaCatalogEntry>(info->propertyschema.c_str())(catalog, this, info, alloc_inst);
	return AddEntry(context, move(propertyschema), info->on_conflict, dependencies);
}

CatalogEntry *SchemaCatalogEntry::CreateExtent(ClientContext &context, CreateExtentInfo *info) {
	unordered_set<CatalogEntry *> dependencies;
	void_allocator alloc_inst (catalog_segment->get_segment_manager());
	auto extent = catalog_segment->find_or_construct<ExtentCatalogEntry>(info->extent.c_str())(catalog, this, info, alloc_inst);
	return AddEntry(context, move(extent), info->on_conflict, dependencies);
}

CatalogEntry *SchemaCatalogEntry::CreateChunkDefinition(ClientContext &context, CreateChunkDefinitionInfo *info) {
	unordered_set<CatalogEntry *> dependencies;
	void_allocator alloc_inst (catalog_segment->get_segment_manager());
	auto chunkdefinition = catalog_segment->find_or_construct<ChunkDefinitionCatalogEntry>(info->chunkdefinition.c_str())(catalog, this, info, alloc_inst);
	return AddEntry(context, move(chunkdefinition), info->on_conflict, dependencies);
}

CatalogEntry *SchemaCatalogEntry::CreateIndex(ClientContext &context, CreateIndexInfo *info) {
	unordered_set<CatalogEntry *> dependencies;
	void_allocator alloc_inst (catalog_segment->get_segment_manager());
	auto index = catalog_segment->find_or_construct<IndexCatalogEntry>(info->index_name.c_str())(catalog, this, info, alloc_inst);
	return AddEntry(context, move(index), info->on_conflict, dependencies);
}

CatalogEntry *SchemaCatalogEntry::CreateFunction(ClientContext &context, CreateFunctionInfo *info) {
	StandardEntry *function;
	void_allocator alloc_inst (catalog_segment->get_segment_manager());
	switch (info->type) {
	case CatalogType::SCALAR_FUNCTION_ENTRY:
		function = catalog_segment->find_or_construct<ScalarFunctionCatalogEntry>(info->name.c_str())(catalog, this, 
																										(CreateScalarFunctionInfo *)info, alloc_inst);
		break;
	case CatalogType::AGGREGATE_FUNCTION_ENTRY:
		D_ASSERT(info->type == CatalogType::AGGREGATE_FUNCTION_ENTRY);
		// create an aggregate function
		function = catalog_segment->find_or_construct<AggregateFunctionCatalogEntry>(info->name.c_str())(catalog, this, 
																										(CreateAggregateFunctionInfo *)info, alloc_inst);
		break;
	default:
		throw InternalException("Unknown function type \"%s\"", CatalogTypeToString(info->type));
	}
	return AddEntry(context, move(function), info->on_conflict);
}

CatalogEntry *SchemaCatalogEntry::AddFunction(ClientContext &context, CreateFunctionInfo *info) {
	D_ASSERT(false); // TODO temporary
	auto entry = GetCatalogSet(info->type).GetEntry(context, info->name);
	if (!entry) {
		return CreateFunction(context, info);
	}

	info->on_conflict = OnCreateConflict::REPLACE_ON_CONFLICT;
	return CreateFunction(context, info);
}

void SchemaCatalogEntry::DropEntry(ClientContext &context, DropInfo *info) {
	auto &set = GetCatalogSet(info->type);

	// first find the entry
	auto existing_entry = set.GetEntry(context, info->name);
	if (!existing_entry) {
		if (!info->if_exists) {
			throw CatalogException("%s with name \"%s\" does not exist!", CatalogTypeToString(info->type), info->name);
		}
		return;
	}
	if (existing_entry->type != info->type) {
		throw CatalogException("Existing object %s is of type %s, trying to replace with type %s", info->name,
		                       CatalogTypeToString(existing_entry->type), CatalogTypeToString(info->type));
	}

	if (!set.DropEntry(context, info->name, info->cascade)) {
		throw InternalException("Could not drop element because of an internal error");
	}
}

void SchemaCatalogEntry::Alter(ClientContext &context, AlterInfo *info) {
	D_ASSERT(false);
}

void SchemaCatalogEntry::Scan(ClientContext &context, CatalogType type,
                              const std::function<void(CatalogEntry *)> &callback) {
	auto &set = GetCatalogSet(type);
	set.Scan(context, callback);
}

void SchemaCatalogEntry::Scan(CatalogType type, const std::function<void(CatalogEntry *)> &callback) {
	auto &set = GetCatalogSet(type);
	set.Scan(callback);
}

void SchemaCatalogEntry::Serialize(Serializer &serializer) {
	D_ASSERT(false);
}

unique_ptr<CreateSchemaInfo> SchemaCatalogEntry::Deserialize(Deserializer &source) {
	D_ASSERT(false);
}

void SchemaCatalogEntry::LoadCatalogSet() {
	graphs.Load(*catalog, catalog_segment, std::string(this->name.data()) + std::string("_graphs"));
	partitions.Load(*catalog, catalog_segment, std::string(this->name.data()) + std::string("_partitions"));
	propertyschemas.Load(*catalog, catalog_segment, std::string(this->name.data()) + std::string("_propertyschemas"));
	extents.Load(*catalog, catalog_segment, std::string(this->name.data()) + std::string("_extents"));
	chunkdefinitions.Load(*catalog, catalog_segment, std::string(this->name.data()) + std::string("_chunkdefinitions"));
	indexes.Load(*catalog, catalog_segment, std::string(this->name.data()) + std::string("_indexes"));
	functions.Load(*catalog, catalog_segment, std::string(this->name.data()) + std::string("_functions"));
}

void SchemaCatalogEntry::SetCatalogSegment(fixed_managed_mapped_file *&catalog_segment) {
	this->catalog_segment = catalog_segment;
}

string SchemaCatalogEntry::ToSQL() {
	std::stringstream ss;
	ss << "CREATE SCHEMA " << name << ";";
	return ss.str();
}

CatalogEntry *SchemaCatalogEntry::GetCatalogEntryFromOid(idx_t oid) {
	auto cat_entry = (CatalogEntry *)oid_to_catalog_entry_array.at(oid);
	return cat_entry;
}

CatalogSet &SchemaCatalogEntry::GetCatalogSet(CatalogType type) {
	switch (type) {
	case CatalogType::GRAPH_ENTRY:
		return graphs;
	case CatalogType::PARTITION_ENTRY:
		return partitions;
	case CatalogType::PROPERTY_SCHEMA_ENTRY:
		return propertyschemas;
	case CatalogType::EXTENT_ENTRY:
		return extents;
	case CatalogType::CHUNKDEFINITION_ENTRY:
		return chunkdefinitions;
	case CatalogType::INDEX_ENTRY:
		return indexes;
	case CatalogType::AGGREGATE_FUNCTION_ENTRY:
	case CatalogType::SCALAR_FUNCTION_ENTRY:
		return functions;
	default:
		throw InternalException("Unsupported catalog type in schema");
	}
}

} // namespace s62
