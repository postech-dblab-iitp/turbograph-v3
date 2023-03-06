#include "catalog/catalog_entry/schema_catalog_entry.hpp"

#include "catalog/catalog.hpp"
#include "catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "catalog/catalog_entry/collate_catalog_entry.hpp"
#include "catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "catalog/catalog_entry/index_catalog_entry.hpp"
#include "catalog/catalog_entry/scalar_macro_catalog_entry.hpp"
#include "catalog/catalog_entry/pragma_function_catalog_entry.hpp"
#include "catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "catalog/catalog_entry/sequence_catalog_entry.hpp"
#include "catalog/catalog_entry/table_catalog_entry.hpp"
#include "catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "catalog/catalog_entry/type_catalog_entry.hpp"
#include "catalog/catalog_entry/graph_catalog_entry.hpp"
#include "catalog/catalog_entry/partition_catalog_entry.hpp"
#include "catalog/catalog_entry/property_schema_catalog_entry.hpp"
#include "catalog/catalog_entry/extent_catalog_entry.hpp"
#include "catalog/catalog_entry/chunkdefinition_catalog_entry.hpp"
//#include "catalog/catalog_entry/view_catalog_entry.hpp"
#include "catalog/default/default_functions.hpp"
//#include "catalog/default/default_views.hpp"
#include "common/exception.hpp"
#include "parser/parsed_data/alter_table_info.hpp"
#include "parser/parsed_data/create_graph_info.hpp"
#include "parser/parsed_data/create_partition_info.hpp"
#include "parser/parsed_data/create_property_schema_info.hpp"
#include "parser/parsed_data/create_extent_info.hpp"
#include "parser/parsed_data/create_chunkdefinition_info.hpp"
//#include "parser/parsed_data/create_collation_info.hpp"
//#include "parser/parsed_data/create_copy_function_info.hpp"
#include "parser/parsed_data/create_index_info.hpp"
//#include "parser/parsed_data/create_pragma_function_info.hpp"
#include "parser/parsed_data/create_scalar_function_info.hpp"
#include "parser/parsed_data/create_schema_info.hpp"
//#include "parser/parsed_data/create_sequence_info.hpp"
//#include "parser/parsed_data/create_table_function_info.hpp"
//#include "parser/parsed_data/create_type_info.hpp"
//#include "parser/parsed_data/create_view_info.hpp"
#include "parser/parsed_data/drop_info.hpp"
//#include "planner/parsed_data/bound_create_table_info.hpp"
//#include "storage/data_table.hpp"
//#include "common/field_writer.hpp"
//#include "planner/constraints/bound_foreign_key_constraint.hpp"
//#include "parser/constraints/foreign_key_constraint.hpp"
#include "catalog/catalog_entry/table_macro_catalog_entry.hpp"

#include <algorithm>
#include <sstream>
#include <iostream>
#include "icecream.hpp"

namespace duckdb {

/*void FindForeignKeyInformation(CatalogEntry *entry, AlterForeignKeyType alter_fk_type,
                               vector<unique_ptr<AlterForeignKeyInfo>> &fk_arrays) {
	if (entry->type != CatalogType::TABLE_ENTRY) {
		return;
	}
	auto *table_entry = (TableCatalogEntry *)entry;
	for (idx_t i = 0; i < table_entry->constraints.size(); i++) {
		auto &cond = table_entry->constraints[i];
		if (cond->type != ConstraintType::FOREIGN_KEY) {
			continue;
		}
		auto &fk = (ForeignKeyConstraint &)*cond;
		if (fk.info.type == ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE) {
			fk_arrays.push_back(make_unique<AlterForeignKeyInfo>(fk.info.schema, fk.info.table, entry->name,
			                                                     fk.pk_columns, fk.fk_columns, fk.info.pk_keys,
			                                                     fk.info.fk_keys, alter_fk_type));
		} else if (fk.info.type == ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE &&
		           alter_fk_type == AlterForeignKeyType::AFT_DELETE) {
			throw CatalogException("Could not drop the table because this table is main key table of the table \"%s\"",
			                       fk.info.table);
		}
	}
}*/

/*SchemaCatalogEntry::SchemaCatalogEntry(Catalog *catalog, string name_p, bool internal)
    : CatalogEntry(CatalogType::SCHEMA_ENTRY, catalog, move(name_p)),
      tables(*catalog, make_unique<DefaultViewGenerator>(*catalog, this)), indexes(*catalog), table_functions(*catalog),
      copy_functions(*catalog), pragma_functions(*catalog),
      functions(*catalog, make_unique<DefaultFunctionGenerator>(*catalog, this)), sequences(*catalog),
      collations(*catalog), types(*catalog) {
	this->internal = internal;
}*/

// SchemaCatalogEntry::SchemaCatalogEntry(Catalog *catalog, string name_p, bool internal)
//     : CatalogEntry(CatalogType::SCHEMA_ENTRY, catalog, move(name_p)), graphs(*catalog), partitions(*catalog),
// 	propertyschemas(*catalog), extents(*catalog), chunkdefinitions(*catalog) {
// 	D_ASSERT(false); // Deprecated
// 	this->internal = internal;
// }

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
IC();
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

/*GraphCatalogEntry *SchemaCatalogEntry::AddGraphEntry(ClientContext &context, graph_unique_ptr_type entry,
                                           OnCreateConflict on_conflict, unordered_set<CatalogEntry *> dependencies) {
	auto entry_name = entry->name;
	auto entry_type = entry->type;
	auto result = boost::interprocess::to_raw_pointer(entry.get());

	if (!AddEntryInternal(context, move(entry.get()), entry_name, entry_type, on_conflict, dependencies)) return nullptr;
	else return result;
}

PartitionCatalogEntry *SchemaCatalogEntry::AddPartitionEntry(ClientContext &context, partition_unique_ptr_type entry,
                                           OnCreateConflict on_conflict, unordered_set<CatalogEntry *> dependencies) {
	auto entry_name = entry->name;
	auto entry_type = entry->type;
	auto result = boost::interprocess::to_raw_pointer(entry.get());

	if (!AddEntryInternal(context, move(entry.get()), entry_name, entry_type, on_conflict, dependencies)) return nullptr;
	else return result;
}

PropertySchemaCatalogEntry *SchemaCatalogEntry::AddPropertySchemaEntry(ClientContext &context, propertyschema_unique_ptr_type entry,
                                           OnCreateConflict on_conflict, unordered_set<CatalogEntry *> dependencies) {
	auto entry_name = entry->name;
	auto entry_type = entry->type;
	auto result = boost::interprocess::to_raw_pointer(entry.get());

	if (!AddEntryInternal(context, move(entry.get()), entry_name, entry_type, on_conflict, dependencies)) return nullptr;
	else return result;
}

ExtentCatalogEntry *SchemaCatalogEntry::AddExtentEntry(ClientContext &context, extent_unique_ptr_type entry,
                                           OnCreateConflict on_conflict, unordered_set<CatalogEntry *> dependencies) {
	auto entry_name = entry->name;
	auto entry_type = entry->type;
	auto result = boost::interprocess::to_raw_pointer(entry.get());

	if (!AddEntryInternal(context, move(entry.get()), entry_name, entry_type, on_conflict, dependencies)) return nullptr;
	else return result;
}

ChunkDefinitionCatalogEntry *SchemaCatalogEntry::AddChunkDefinitionEntry(ClientContext &context, chunkdefinition_unique_ptr_type entry,
                                           OnCreateConflict on_conflict, unordered_set<CatalogEntry *> dependencies) {
	auto entry_name = entry->name;
	auto entry_type = entry->type;
	auto result = boost::interprocess::to_raw_pointer(entry.get());

	if (!AddEntryInternal(context, move(entry.get()), entry_name, entry_type, on_conflict, dependencies)) return nullptr;
	else return result;
}*/

CatalogEntry *SchemaCatalogEntry::CreateGraph(ClientContext &context, CreateGraphInfo *info) {
	unordered_set<CatalogEntry *> dependencies;
	void_allocator alloc_inst (catalog_segment->get_segment_manager());
	auto graph = catalog_segment->find_or_construct<GraphCatalogEntry>(info->graph.c_str())(catalog, this, info, alloc_inst);
	//auto graph = boost::interprocess::make_managed_unique_ptr(
	//	catalog_segment->construct<GraphCatalogEntry>(info->graph.c_str())(catalog, this, info),
	//	*catalog_segment);
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

/*
CatalogEntry *SchemaCatalogEntry::CreateSequence(ClientContext &context, CreateSequenceInfo *info) {
	auto sequence = make_unique<SequenceCatalogEntry>(catalog, this, info);
	return AddEntry(context, move(sequence), info->on_conflict);
}

CatalogEntry *SchemaCatalogEntry::CreateType(ClientContext &context, CreateTypeInfo *info) {
	auto sequence = make_unique<TypeCatalogEntry>(catalog, this, info);
	return AddEntry(context, move(sequence), info->on_conflict);
}

CatalogEntry *SchemaCatalogEntry::CreateTable(ClientContext &context, BoundCreateTableInfo *info) {
	auto table = make_unique<TableCatalogEntry>(catalog, this, info);
	table->storage->info->cardinality = table->storage->GetTotalRows();
	CatalogEntry *entry = AddEntry(context, move(table), info->Base().on_conflict, info->dependencies);
	if (!entry) {
		return nullptr;
	}
	// add a foreign key constraint in main key table if there is a foreign key constraint
	vector<unique_ptr<AlterForeignKeyInfo>> fk_arrays;
	FindForeignKeyInformation(entry, AlterForeignKeyType::AFT_ADD, fk_arrays);
	for (idx_t i = 0; i < fk_arrays.size(); i++) {
		// alter primary key table
		AlterForeignKeyInfo *fk_info = fk_arrays[i].get();
		catalog->Alter(context, fk_info);

		// make a dependency between this table and referenced table
		auto &set = GetCatalogSet(CatalogType::TABLE_ENTRY);
		info->dependencies.insert(set.GetEntry(context, fk_info->name));
	}
	return entry;
}

CatalogEntry *SchemaCatalogEntry::CreateView(ClientContext &context, CreateViewInfo *info) {
	auto view = make_unique<ViewCatalogEntry>(catalog, this, info);
	return AddEntry(context, move(view), info->on_conflict);
}
*/

/*
CatalogEntry *SchemaCatalogEntry::CreateCollation(ClientContext &context, CreateCollationInfo *info) {
	auto collation = make_unique<CollateCatalogEntry>(catalog, this, info);
	return AddEntry(context, move(collation), info->on_conflict);
}

CatalogEntry *SchemaCatalogEntry::CreateTableFunction(ClientContext &context, CreateTableFunctionInfo *info) {
	auto table_function = make_unique<TableFunctionCatalogEntry>(catalog, this, info);
	return AddEntry(context, move(table_function), info->on_conflict);
}

CatalogEntry *SchemaCatalogEntry::CreateCopyFunction(ClientContext &context, CreateCopyFunctionInfo *info) {
	auto copy_function = make_unique<CopyFunctionCatalogEntry>(catalog, this, info);
	return AddEntry(context, move(copy_function), info->on_conflict);
}

CatalogEntry *SchemaCatalogEntry::CreatePragmaFunction(ClientContext &context, CreatePragmaFunctionInfo *info) {
	auto pragma_function = make_unique<PragmaFunctionCatalogEntry>(catalog, this, info);
	return AddEntry(context, move(pragma_function), info->on_conflict);
}*/

CatalogEntry *SchemaCatalogEntry::CreateFunction(ClientContext &context, CreateFunctionInfo *info) {
	// unique_ptr<StandardEntry> function;
	StandardEntry *function;
	void_allocator alloc_inst (catalog_segment->get_segment_manager());
	switch (info->type) {
	case CatalogType::SCALAR_FUNCTION_ENTRY:
		function = catalog_segment->find_or_construct<ScalarFunctionCatalogEntry>(info->name.c_str())(catalog, this, 
																										(CreateScalarFunctionInfo *)info, alloc_inst);
		break;
	case CatalogType::MACRO_ENTRY:
		D_ASSERT(false);
		// create a macro function
		// function = make_unique_base<StandardEntry, ScalarMacroCatalogEntry>(catalog, this, (CreateMacroInfo *)info);
		break;

	case CatalogType::TABLE_MACRO_ENTRY:
		D_ASSERT(false);
		// create a macro function
		// function = make_unique_base<StandardEntry, TableMacroCatalogEntry>(catalog, this, (CreateMacroInfo *)info);
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
	// switch (info->type) {
	// case CatalogType::SCALAR_FUNCTION_ENTRY: {
	// 	auto scalar_info = (CreateScalarFunctionInfo *)info;
	// 	auto &scalars = *(ScalarFunctionCatalogEntry *)entry;
	// 	for (const auto &scalar : scalars.functions) {
	// 		scalar_info->functions.emplace_back(scalar);
	// 	}
	// 	break;
	// }
	// case CatalogType::AGGREGATE_FUNCTION_ENTRY: {
	// 	auto agg_info = (CreateAggregateFunctionInfo *)info;
	// 	auto &aggs = *(AggregateFunctionCatalogEntry *)entry;
	// 	for (const auto &agg : aggs.functions) {
	// 		agg_info->functions.AddFunction(agg);
	// 	}
	// 	break;
	// }
	// default:
	// 	// Macros can only be replaced because there is only one of each name.
	// 	throw InternalException("Unsupported function type \"%s\" for adding", CatalogTypeToString(info->type));
	// }
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

	//// if there is a foreign key constraint, get that information
	//vector<unique_ptr<AlterForeignKeyInfo>> fk_arrays;
	//FindForeignKeyInformation(existing_entry, AlterForeignKeyType::AFT_DELETE, fk_arrays);

	if (!set.DropEntry(context, info->name, info->cascade)) {
		throw InternalException("Could not drop element because of an internal error");
	}

	// remove the foreign key constraint in main key table if main key table's name is valid
	/*for (idx_t i = 0; i < fk_arrays.size(); i++) {
		// alter primary key tablee
		Catalog::GetCatalog(context).Alter(context, fk_arrays[i].get());
	}*/
}

void SchemaCatalogEntry::Alter(ClientContext &context, AlterInfo *info) {
	D_ASSERT(false);
	/*CatalogType type = info->GetCatalogType();
	auto &set = GetCatalogSet(type);
	if (info->type == AlterType::CHANGE_OWNERSHIP) {
		if (!set.AlterOwnership(context, (ChangeOwnershipInfo *)info)) {
			throw CatalogException("Couldn't change ownership!");
		}
	} else {
		string name = info->name;
		if (!set.AlterEntry(context, name, info)) {
			throw CatalogException("Entry with name \"%s\" does not exist!", name);
		}
	}*/
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
	//FieldWriter writer(serializer);
	//writer.WriteString(name);
	//writer.Finalize();
}

unique_ptr<CreateSchemaInfo> SchemaCatalogEntry::Deserialize(Deserializer &source) {
	D_ASSERT(false);
	//auto info = make_unique<CreateSchemaInfo>();

	//FieldReader reader(source);
	//info->schema = reader.ReadRequired<string>();
	//reader.Finalize();

	//return info;
}

void SchemaCatalogEntry::LoadCatalogSet() {
	// fprintf(stdout, "Load Graph Catalog\n");
	graphs.Load(*catalog, catalog_segment, std::string(this->name.data()) + std::string("_graphs"));
	// fprintf(stdout, "Load Partition Catalog\n");
	partitions.Load(*catalog, catalog_segment, std::string(this->name.data()) + std::string("_partitions"));
	// fprintf(stdout, "Load PropertySchema Catalog\n");
	propertyschemas.Load(*catalog, catalog_segment, std::string(this->name.data()) + std::string("_propertyschemas"));
	// fprintf(stdout, "Load Extent Catalog\n");
	extents.Load(*catalog, catalog_segment, std::string(this->name.data()) + std::string("_extents"));
	// fprintf(stdout, "Load ChunkDefinitions Catalog\n");
	chunkdefinitions.Load(*catalog, catalog_segment, std::string(this->name.data()) + std::string("_chunkdefinitions"));
	// fprintf(stdout, "Load Indexes Catalog\n");
	indexes.Load(*catalog, catalog_segment, std::string(this->name.data()) + std::string("_indexes"));
	// fprintf(stdout, "Load Functions Catalog\n");
	functions.Load(*catalog, catalog_segment, std::string(this->name.data()) + std::string("_functions"));
	// fprintf(stdout, "Load CatalogSet Done\n");
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
	case CatalogType::MACRO_ENTRY:
		return functions;
	/*case CatalogType::VIEW_ENTRY:
	case CatalogType::TABLE_ENTRY:
		return tables;
	case CatalogType::TABLE_FUNCTION_ENTRY:
	case CatalogType::TABLE_MACRO_ENTRY:
		return table_functions;
	case CatalogType::COPY_FUNCTION_ENTRY:
		return copy_functions;
	case CatalogType::PRAGMA_FUNCTION_ENTRY:
		return pragma_functions;
	case CatalogType::SEQUENCE_ENTRY:
		return sequences;
	case CatalogType::COLLATION_ENTRY:
		return collations;
	case CatalogType::TYPE_ENTRY:
		return types;*/
	default:
		throw InternalException("Unsupported catalog type in schema");
	}
}

} // namespace duckdb
