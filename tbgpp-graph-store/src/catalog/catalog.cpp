#include "catalog/catalog.hpp"

//#include "catalog/catalog_search_path.hpp"
#include "catalog/catalog_entry/list.hpp"
#include "catalog/catalog_set.hpp"
#include "catalog/default/default_schemas.hpp"
#include "catalog/dependency_manager.hpp"
#include "common/exception.hpp"
#include "main/client_context.hpp"
#include "main/client_data.hpp"
#include "main/database.hpp"
//#include "parser/expression/function_expression.hpp"

#include "parser/parsed_data/alter_table_info.hpp"
#include "parser/parsed_data/create_graph_info.hpp"
#include "parser/parsed_data/create_partition_info.hpp"
#include "parser/parsed_data/create_property_schema_info.hpp"
#include "parser/parsed_data/create_extent_info.hpp"
#include "parser/parsed_data/create_chunkdefinition_info.hpp"
#include "parser/parsed_data/create_aggregate_function_info.hpp"
#include "parser/parsed_data/create_scalar_function_info.hpp"
#include "parser/parsed_data/create_index_info.hpp"
/*
#include "parser/parsed_data/create_collation_info.hpp"
#include "parser/parsed_data/create_copy_function_info.hpp"
#include "parser/parsed_data/create_pragma_function_info.hpp"
*/
#include "parser/parsed_data/create_schema_info.hpp"
/*
#include "parser/parsed_data/create_sequence_info.hpp"
#include "parser/parsed_data/create_table_function_info.hpp"
#include "parser/parsed_data/create_type_info.hpp"
#include "parser/parsed_data/create_view_info.hpp"
*/
#include "parser/parsed_data/drop_info.hpp"
//#include "planner/parsed_data/bound_create_table_info.hpp"
//#include "planner/binder.hpp"

#include <algorithm>
#include <iostream>
#include <filesystem>
#include <fstream>

#include "icecream.hpp"	
namespace duckdb {

string SimilarCatalogEntry::GetQualifiedName() const {
	D_ASSERT(Found());

	return std::string(schema->name.data()) + "." + name;
}

Catalog::Catalog(DatabaseInstance &db)
    : db(db), dependency_manager(make_unique<DependencyManager>(*this)) {
	catalog_version = 1; // TODO we need to load this
}

Catalog::Catalog(DatabaseInstance &db, fixed_managed_mapped_file *&catalog_segment_)
    : db(db), schemas(make_unique<CatalogSet>(*this, catalog_segment_, "schemas", make_unique<DefaultSchemaGenerator>(*this))),
      dependency_manager(make_unique<DependencyManager>(*this)) {
	catalog_version = 1;
	catalog_segment = catalog_segment_;
	
	// create the default schema
	std::shared_ptr<ClientContext> client = 
		std::make_shared<ClientContext>(db.shared_from_this());
	CreateSchemaInfo schema_info;
	CreateSchema(*client.get(), &schema_info);

	// initialize default functions
	BuiltinFunctions builtin(*client.get(), *this, false);
	builtin.Initialize();
}

Catalog::~Catalog() {
}

void Catalog::LoadCatalog(fixed_managed_mapped_file *&catalog_segment_, vector<vector<string>> &object_names, string path) {
	schemas = make_unique<CatalogSet>(*this, catalog_segment_, "schemas", make_unique<DefaultSchemaGenerator>(*this));
	catalog_segment = catalog_segment_;

	// Load SchemaCatalogEntry
	unordered_set<CatalogEntry *> dependencies;
	string schema_cat_name_in_shm = "schemacatalogentry_main"; // XXX currently, we assume there is only one schema
	auto entry = this->catalog_segment->find_or_construct<SchemaCatalogEntry>(schema_cat_name_in_shm.c_str()) (this, "main", false, this->catalog_segment);
	entry->SetCatalog(this);

	std::shared_ptr<ClientContext> client = 
		std::make_shared<ClientContext>(db.shared_from_this());
	if (!schemas->CreateEntry(*client.get(), "main", move(entry), dependencies)) {
		throw CatalogException("Schema with name main already exists!");
	}

	// Set SHM
	entry->SetCatalogSegment(catalog_segment_);

	// Load CatalogSet
	entry->LoadCatalogSet();

	// initialize default functions
	BuiltinFunctions builtin(*client.get(), *this, true);
	builtin.Initialize();

	if (std::filesystem::exists(path + "/catalog_version")) {
		string catalog_version_str;
		std::ifstream ifs;
		ifs.open(path + "/catalog_version", std::fstream::in);
        if (ifs.is_open()) {
            ifs.seekg(-2, std::ios_base::end);  // go to one spot before the EOF

            bool keepLooping = true;
            while (keepLooping) {
                char ch;
                ifs.get(ch);

                if ((int)ifs.tellg() <= 1) {
                    ifs.seekg(0);
                    keepLooping = false;
                }
                else if (ch == '\n') {
                    keepLooping = false;
                }
                else {
                    ifs.seekg(-2, std::ios_base::cur);
                }
            }

            std::getline(ifs, catalog_version_str);
            ifs.close();
        }

        catalog_version = std::stoll(catalog_version_str);
		std::cout << "catalog_version: " << catalog_version << std::endl;

		ofs = new std::ofstream();
		ofs->open(path + "/catalog_version", std::ofstream::out | std::ofstream::ate);
		*ofs << std::to_string(catalog_version) + "\n" << std::flush;
	} else {
		catalog_version = 10000000; // temporary..
		ofs = new std::ofstream();
		ofs->open(path + "/catalog_version", std::ofstream::out | std::ofstream::trunc);
		*ofs << std::to_string(catalog_version) + "\n" << std::flush;
	}
}

Catalog &Catalog::GetCatalog(ClientContext &context) {
	return context.db->GetCatalog();
}

CatalogEntry *Catalog::CreateGraph(ClientContext &context, CreateGraphInfo *info) {
	/*const_named_it named_beg = catalog_segment->named_begin();
	const_named_it named_end = catalog_segment->named_end();
	fprintf(stdout, "All named object list\n");
	for(; named_beg != named_end; ++named_beg){
		//A pointer to the name of the named object
		const boost::interprocess::managed_shared_memory::char_type *name = named_beg->name();
		fprintf(stdout, "\t%s\n", name);
	}*/
	auto schema = GetSchema(context, info->schema);
	return CreateGraph(context, schema, info);
}

CatalogEntry *Catalog::CreateGraph(ClientContext &context, SchemaCatalogEntry *schema, CreateGraphInfo *info) {
	return schema->CreateGraph(context, info);
}

CatalogEntry *Catalog::CreatePartition(ClientContext &context, CreatePartitionInfo *info) {
	auto schema = GetSchema(context, info->schema);
	return CreatePartition(context, schema, info);
}

CatalogEntry *Catalog::CreatePartition(ClientContext &context, SchemaCatalogEntry *schema, CreatePartitionInfo *info) {
	return schema->CreatePartition(context, info);
}

CatalogEntry *Catalog::CreatePropertySchema(ClientContext &context, CreatePropertySchemaInfo *info) {
	auto schema = GetSchema(context, info->schema);
	return CreatePropertySchema(context, schema, info);
}

CatalogEntry *Catalog::CreatePropertySchema(ClientContext &context, SchemaCatalogEntry *schema, CreatePropertySchemaInfo *info) {
	return schema->CreatePropertySchema(context, info);
}

CatalogEntry *Catalog::CreateExtent(ClientContext &context, CreateExtentInfo *info) {
	auto schema = GetSchema(context, info->schema);
	return CreateExtent(context, schema, info);
}

CatalogEntry *Catalog::CreateExtent(ClientContext &context, SchemaCatalogEntry *schema, CreateExtentInfo *info) {
	return schema->CreateExtent(context, info);
}

CatalogEntry *Catalog::CreateChunkDefinition(ClientContext &context, CreateChunkDefinitionInfo *info) {
	auto schema = GetSchema(context, info->schema);
	return CreateChunkDefinition(context, schema, info);
}

CatalogEntry *Catalog::CreateChunkDefinition(ClientContext &context, SchemaCatalogEntry *schema, CreateChunkDefinitionInfo *info) {
	return schema->CreateChunkDefinition(context, info);
}

CatalogEntry *Catalog::CreateFunction(ClientContext &context, CreateFunctionInfo *info) {
	auto schema = GetSchema(context, info->schema);
	return CreateFunction(context, schema, info);
}

CatalogEntry *Catalog::CreateFunction(ClientContext &context, SchemaCatalogEntry *schema, CreateFunctionInfo *info) {
	return schema->CreateFunction(context, info);
}

CatalogEntry *Catalog::CreateIndex(ClientContext &context, CreateIndexInfo *info) {
	auto schema = GetSchema(context, info->schema);
	return CreateIndex(context, schema, info);
}

CatalogEntry *Catalog::CreateIndex(ClientContext &context, SchemaCatalogEntry *schema, CreateIndexInfo *info) {
	return schema->CreateIndex(context, info);
}

/*CatalogEntry *Catalog::CreateTable(ClientContext &context, BoundCreateTableInfo *info) {
	auto schema = GetSchema(context, info->base->schema);
	return CreateTable(context, schema, info);
}

CatalogEntry *Catalog::CreateTable(ClientContext &context, unique_ptr<CreateTableInfo> info) {
	auto binder = Binder::CreateBinder(context);
	auto bound_info = binder->BindCreateTableInfo(move(info));
	return CreateTable(context, bound_info.get());
}

CatalogEntry *Catalog::CreateTable(ClientContext &context, SchemaCatalogEntry *schema, BoundCreateTableInfo *info) {
	return schema->CreateTable(context, info);
}

CatalogEntry *Catalog::CreateView(ClientContext &context, CreateViewInfo *info) {
	auto schema = GetSchema(context, info->schema);
	return CreateView(context, schema, info);
}

CatalogEntry *Catalog::CreateView(ClientContext &context, SchemaCatalogEntry *schema, CreateViewInfo *info) {
	return schema->CreateView(context, info);
}

CatalogEntry *Catalog::CreateSequence(ClientContext &context, CreateSequenceInfo *info) {
	auto schema = GetSchema(context, info->schema);
	return CreateSequence(context, schema, info);
}

CatalogEntry *Catalog::CreateType(ClientContext &context, CreateTypeInfo *info) {
	auto schema = GetSchema(context, info->schema);
	return CreateType(context, schema, info);
}

CatalogEntry *Catalog::CreateSequence(ClientContext &context, SchemaCatalogEntry *schema, CreateSequenceInfo *info) {
	return schema->CreateSequence(context, info);
}

CatalogEntry *Catalog::CreateType(ClientContext &context, SchemaCatalogEntry *schema, CreateTypeInfo *info) {
	return schema->CreateType(context, info);
}

CatalogEntry *Catalog::CreateTableFunction(ClientContext &context, CreateTableFunctionInfo *info) {
	auto schema = GetSchema(context, info->schema);
	return CreateTableFunction(context, schema, info);
}

CatalogEntry *Catalog::CreateTableFunction(ClientContext &context, SchemaCatalogEntry *schema,
                                           CreateTableFunctionInfo *info) {
	return schema->CreateTableFunction(context, info);
}

CatalogEntry *Catalog::CreateCopyFunction(ClientContext &context, CreateCopyFunctionInfo *info) {
	auto schema = GetSchema(context, info->schema);
	return CreateCopyFunction(context, schema, info);
}

CatalogEntry *Catalog::CreateCopyFunction(ClientContext &context, SchemaCatalogEntry *schema,
                                          CreateCopyFunctionInfo *info) {
	return schema->CreateCopyFunction(context, info);
}

CatalogEntry *Catalog::CreatePragmaFunction(ClientContext &context, CreatePragmaFunctionInfo *info) {
	auto schema = GetSchema(context, info->schema);
	return CreatePragmaFunction(context, schema, info);
}

CatalogEntry *Catalog::CreatePragmaFunction(ClientContext &context, SchemaCatalogEntry *schema,
                                            CreatePragmaFunctionInfo *info) {
	return schema->CreatePragmaFunction(context, info);
}

CatalogEntry *Catalog::CreateCollation(ClientContext &context, CreateCollationInfo *info) {
	auto schema = GetSchema(context, info->schema);
	return CreateCollation(context, schema, info);
}

CatalogEntry *Catalog::CreateCollation(ClientContext &context, SchemaCatalogEntry *schema, CreateCollationInfo *info) {
	return schema->CreateCollation(context, info);
}*/

CatalogEntry *Catalog::CreateSchema(ClientContext &context, CreateSchemaInfo *info) {
	D_ASSERT(!info->schema.empty());
	if (info->schema == TEMP_SCHEMA) {
		throw CatalogException("Cannot create built-in schema \"%s\"", info->schema);
	}

	unordered_set<CatalogEntry *> dependencies;
	string schema_cat_name_in_shm = "schemacatalogentry_" + info->schema;
	auto entry = this->catalog_segment->construct<SchemaCatalogEntry>(schema_cat_name_in_shm.c_str()) (this, info->schema, info->internal, this->catalog_segment);
	// fprintf(stdout, "Create Schema %s, %p\n", schema_cat_name_in_shm.c_str(), entry);
	// const_named_it named_beg = catalog_segment->named_begin();
	// const_named_it named_end = catalog_segment->named_end();
	// fprintf(stdout, "All named object list\n");
	// for(; named_beg != named_end; ++named_beg){
	// 	//A pointer to the name of the named object
	// 	const boost::interprocess::managed_shared_memory::char_type *name = named_beg->name();
	// 	fprintf(stdout, "\t%s %p\n", name, named_beg->value());
	// }
	std::pair<SchemaCatalogEntry *,std::size_t> ret = catalog_segment->find<SchemaCatalogEntry>("schemacatalogentry_main");
	SchemaCatalogEntry *schema_cat = ret.first;
	auto result = (CatalogEntry*) entry;
	//auto entry = boost::interprocess::make_managed_unique_ptr(this->catalog_segment->construct<SchemaCatalogEntry>(schema_cat_name_in_shm.c_str())
	//		(this, info->schema, info->internal, this->catalog_segment), *this->catalog_segment);
	//auto result = boost::interprocess::to_raw_pointer(entry.get());
	//if (!schemas->CreateEntry(context, info->schema, move(entry.get()), dependencies)) {
	if (!schemas->CreateEntry(context, info->schema, move(entry), dependencies)) {
		if (info->on_conflict == OnCreateConflict::ERROR_ON_CONFLICT) {
			throw CatalogException("Schema with name %s already exists!", info->schema);
		} else {
			D_ASSERT(info->on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT);
		}
		return nullptr;
	}
	return result;
}

void Catalog::DropSchema(ClientContext &context, DropInfo *info) {
	D_ASSERT(!info->name.empty());
	ModifyCatalog();
	if (!schemas->DropEntry(context, info->name, info->cascade)) {
		if (!info->if_exists) {
			throw CatalogException("Schema with name \"%s\" does not exist!", info->name);
		}
	}
}

void Catalog::DropEntry(ClientContext &context, DropInfo *info) {
	ModifyCatalog();
	if (info->type == CatalogType::SCHEMA_ENTRY) {
		// DROP SCHEMA
		DropSchema(context, info);
		return;
	}

	auto lookup = LookupEntry(context, info->type, info->schema, info->name, info->if_exists);
	if (!lookup.Found()) {
		return;
	}

	lookup.schema->DropEntry(context, info);
}

CatalogEntry *Catalog::AddFunction(ClientContext &context, CreateFunctionInfo *info) {
	auto schema = GetSchema(context, info->schema);
	return AddFunction(context, schema, info);
}

CatalogEntry *Catalog::AddFunction(ClientContext &context, SchemaCatalogEntry *schema, CreateFunctionInfo *info) {
	return schema->AddFunction(context, info);
}

SchemaCatalogEntry *Catalog::GetSchema(ClientContext &context, const string &schema_name, bool if_exists) {
                                       //QueryErrorContext error_context) {
	D_ASSERT(!schema_name.empty());
	if (schema_name == TEMP_SCHEMA) {
		D_ASSERT(false);
		//return ClientData::Get(context).temporary_objects.get();
	}
// IC();
	auto entry = schemas->GetEntry(context, schema_name);
	if (!entry && !if_exists) {
		D_ASSERT(false); // TODO exception handling
		//throw CatalogException(error_context.FormatError("Schema with name %s does not exist!", schema_name));
	}
	//fprintf(stdout, "GetSchema %p\n", entry);
	return (SchemaCatalogEntry *)entry;
}

void Catalog::ScanSchemas(ClientContext &context, std::function<void(CatalogEntry *)> callback) {
	// create all default schemas first
	schemas->Scan(context, [&](CatalogEntry *entry) { callback(entry); });
}

SimilarCatalogEntry Catalog::SimilarEntryInSchemas(ClientContext &context, const string &entry_name, CatalogType type,
                                                   const vector<SchemaCatalogEntry *> &schemas) {

	vector<CatalogSet *> sets;
	std::transform(schemas.begin(), schemas.end(), std::back_inserter(sets),
	               [type](SchemaCatalogEntry *s) -> CatalogSet * { return &s->GetCatalogSet(type); });
	pair<string, idx_t> most_similar {"", (idx_t)-1};
	SchemaCatalogEntry *schema_of_most_similar = nullptr;
	for (auto schema : schemas) {
		auto entry = schema->GetCatalogSet(type).SimilarEntry(context, entry_name);
		if (!entry.first.empty() && (most_similar.first.empty() || most_similar.second > entry.second)) {
			most_similar = entry;
			schema_of_most_similar = schema;
		}
	}

	return {most_similar.first, most_similar.second, schema_of_most_similar};
}

/*CatalogException Catalog::CreateMissingEntryException(ClientContext &context, const string &entry_name,
                                                      CatalogType type, const vector<SchemaCatalogEntry *> &schemas) {
                                                      //QueryErrorContext error_context) {
	auto entry = SimilarEntryInSchemas(context, entry_name, type, schemas);

	vector<SchemaCatalogEntry *> unseen_schemas;
	this->schemas->Scan([&schemas, &unseen_schemas](CatalogEntry *entry) {
		auto schema_entry = (SchemaCatalogEntry *)entry;
		if (std::find(schemas.begin(), schemas.end(), schema_entry) == schemas.end()) {
			unseen_schemas.emplace_back(schema_entry);
		}
	});
	auto unseen_entry = SimilarEntryInSchemas(context, entry_name, type, unseen_schemas);

	string did_you_mean;
	if (unseen_entry.Found() && unseen_entry.distance < entry.distance) {
		did_you_mean = "\nDid you mean \"" + unseen_entry.GetQualifiedName() + "\"?";
	} else if (entry.Found()) {
		did_you_mean = "\nDid you mean \"" + entry.name + "\"?";
	}

	return CatalogException(error_context.FormatError("%s with name %s does not exist!%s", CatalogTypeToString(type),
	                                                  entry_name, did_you_mean));
}*/

CatalogEntryLookup Catalog::LookupEntry(ClientContext &context, CatalogType type, const string &schema_name,
                                        const string &name, bool if_exists) { //, QueryErrorContext error_context) {
	if (!schema_name.empty()) {
		auto schema = GetSchema(context, schema_name, if_exists);//, error_context);

		if (!schema) {
			D_ASSERT(if_exists);
			return {nullptr, nullptr};
		}

		auto entry = schema->GetCatalogSet(type).GetEntry(context, name);

		if (!entry && !if_exists) {
			D_ASSERT(false);
			//throw CreateMissingEntryException(context, name, type, {schema}, error_context);
		}

		return {schema, entry};
	}

//	const auto &paths = ClientData::Get(context).catalog_search_path->Get();
	const auto paths = vector<string>();
	for (const auto &path : paths) {
		//auto lookup = LookupEntry(context, type, path, name, true, error_context);
		auto lookup = LookupEntry(context, type, path, name, true);
		if (lookup.Found()) {
			return lookup;
		}
	}

	if (!if_exists) {
		vector<SchemaCatalogEntry *> schemas;
		for (const auto &path : paths) {
			auto schema = GetSchema(context, path, true);
			if (schema) {
				schemas.emplace_back(schema);
			}
		}

		D_ASSERT(false);
		//throw CreateMissingEntryException(context, name, type, schemas, error_context);
	}

	return {nullptr, nullptr};
}

CatalogEntry *Catalog::GetEntry(ClientContext &context, const string &schema, const string &name) {
	D_ASSERT(false);
	vector<CatalogType> entry_types {CatalogType::TABLE_ENTRY, CatalogType::SEQUENCE_ENTRY};

	for (auto entry_type : entry_types) {
		CatalogEntry *result = GetEntry(context, entry_type, schema, name, true);
		if (result != nullptr) {
			return result;
		}
	}

	throw CatalogException("CatalogElement \"%s.%s\" does not exist!", schema, name);
}

CatalogEntry *Catalog::GetEntry(ClientContext &context, CatalogType type, const string &schema_name, const string &name,
                                bool if_exists) { //, QueryErrorContext error_context) {
	//return LookupEntry(context, type, schema_name, name, if_exists, error_context).entry;
	return LookupEntry(context, type, schema_name, name, if_exists).entry;
}

CatalogEntry *Catalog::GetEntry(ClientContext &context, const string &schema_name, idx_t oid, bool if_exists) {
	if (!schema_name.empty()) {
		auto schema = GetSchema(context, schema_name, if_exists);//, error_context);

		if (!schema) {
			D_ASSERT(if_exists);
			return nullptr;
		}

		auto entry = schema->GetCatalogEntryFromOid(oid);

		if (!entry && !if_exists) {
			D_ASSERT(false);
			//throw CreateMissingEntryException(context, name, type, {schema}, error_context);
		}

		return entry;
	}

//	const auto &paths = ClientData::Get(context).catalog_search_path->Get();
// TODO remove logics using search paths..

	return nullptr;
}

template <>
GraphCatalogEntry *Catalog::GetEntry(ClientContext &context, const string &schema_name, const string &name,
                                     bool if_exists) { //, QueryErrorContext error_context) {
	return (GraphCatalogEntry*) GetEntry(context, CatalogType::GRAPH_ENTRY, schema_name, name, if_exists);
}

template <>
PartitionCatalogEntry *Catalog::GetEntry(ClientContext &context, const string &schema_name, const string &name,
                                     bool if_exists) { //, QueryErrorContext error_context) {
return (PartitionCatalogEntry*) GetEntry(context, CatalogType::PARTITION_ENTRY, schema_name, name, if_exists);
}

template <>
PropertySchemaCatalogEntry *Catalog::GetEntry(ClientContext &context, const string &schema_name, const string &name,
                                     bool if_exists) { //, QueryErrorContext error_context) {
return (PropertySchemaCatalogEntry*) GetEntry(context, CatalogType::PROPERTY_SCHEMA_ENTRY, schema_name, name, if_exists);
}

template <>
ExtentCatalogEntry *Catalog::GetEntry(ClientContext &context, const string &schema_name, const string &name,
                                     bool if_exists) { //, QueryErrorContext error_context) {
return (ExtentCatalogEntry*) GetEntry(context, CatalogType::EXTENT_ENTRY, schema_name, name, if_exists);
}

template <>
ChunkDefinitionCatalogEntry *Catalog::GetEntry(ClientContext &context, const string &schema_name, const string &name,
                                     bool if_exists) { //, QueryErrorContext error_context) {
return (ChunkDefinitionCatalogEntry*) GetEntry(context, CatalogType::CHUNKDEFINITION_ENTRY, schema_name, name, if_exists);
}


/*
template <>
TableCatalogEntry *Catalog::GetEntry(ClientContext &context, const string &schema_name, const string &name,
                                     bool if_exists, QueryErrorContext error_context) {
	auto entry = GetEntry(context, CatalogType::TABLE_ENTRY, schema_name, name, if_exists);
	if (!entry) {
		return nullptr;
	}
	if (entry->type != CatalogType::TABLE_ENTRY) {
		throw CatalogException(error_context.FormatError("%s is not a table", name));
	}
	return (TableCatalogEntry *)entry;
}

template <>
SequenceCatalogEntry *Catalog::GetEntry(ClientContext &context, const string &schema_name, const string &name,
                                        bool if_exists, QueryErrorContext error_context) {
	return (SequenceCatalogEntry *)GetEntry(context, CatalogType::SEQUENCE_ENTRY, schema_name, name, if_exists,
	                                        error_context);
}

template <>
TableFunctionCatalogEntry *Catalog::GetEntry(ClientContext &context, const string &schema_name, const string &name,
                                             bool if_exists, QueryErrorContext error_context) {
	return (TableFunctionCatalogEntry *)GetEntry(context, CatalogType::TABLE_FUNCTION_ENTRY, schema_name, name,
	                                             if_exists, error_context);
}

template <>
CopyFunctionCatalogEntry *Catalog::GetEntry(ClientContext &context, const string &schema_name, const string &name,
                                            bool if_exists, QueryErrorContext error_context) {
	return (CopyFunctionCatalogEntry *)GetEntry(context, CatalogType::COPY_FUNCTION_ENTRY, schema_name, name, if_exists,
	                                            error_context);
}

template <>
PragmaFunctionCatalogEntry *Catalog::GetEntry(ClientContext &context, const string &schema_name, const string &name,
                                              bool if_exists, QueryErrorContext error_context) {
	return (PragmaFunctionCatalogEntry *)GetEntry(context, CatalogType::PRAGMA_FUNCTION_ENTRY, schema_name, name,
	                                              if_exists, error_context);
}

template <>
AggregateFunctionCatalogEntry *Catalog::GetEntry(ClientContext &context, const string &schema_name, const string &name,
                                                 bool if_exists, QueryErrorContext error_context) {
	auto entry = GetEntry(context, CatalogType::AGGREGATE_FUNCTION_ENTRY, schema_name, name, if_exists, error_context);
	if (entry->type != CatalogType::AGGREGATE_FUNCTION_ENTRY) {
		throw CatalogException(error_context.FormatError("%s is not an aggregate function", name));
	}
	return (AggregateFunctionCatalogEntry *)entry;
}

template <>
CollateCatalogEntry *Catalog::GetEntry(ClientContext &context, const string &schema_name, const string &name,
                                       bool if_exists, QueryErrorContext error_context) {
	return (CollateCatalogEntry *)GetEntry(context, CatalogType::COLLATION_ENTRY, schema_name, name, if_exists,
	                                       error_context);
}
*/

void Catalog::Alter(ClientContext &context, AlterInfo *info) {
	D_ASSERT(false);
	ModifyCatalog();
	auto lookup = LookupEntry(context, info->GetCatalogType(), info->schema, info->name);
	D_ASSERT(lookup.Found()); // It must have thrown otherwise.
	return lookup.schema->Alter(context, info);
}

idx_t Catalog::GetCatalogVersion() {
	return catalog_version;
}

idx_t Catalog::ModifyCatalog() {
	if (ofs) {
		// TODO
		*ofs << std::to_string(catalog_version + 1) + "\n" << std::flush;
	}
	return catalog_version++;
}

} // namespace duckdb
