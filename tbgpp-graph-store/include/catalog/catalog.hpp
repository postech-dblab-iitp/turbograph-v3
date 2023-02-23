//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/catalog_entry.hpp"
#include "common/mutex.hpp"
//#include "duckdb/parser/query_error_context.hpp"

#include <functional>
#include "common/atomic.hpp"
#include "common/boost.hpp"
#include "common/boost_typedefs.hpp"

namespace duckdb {
struct CreateSchemaInfo;
struct DropInfo;
struct BoundCreateTableInfo;
struct AlterTableInfo;
struct CreateTableFunctionInfo;
struct CreateCopyFunctionInfo;
struct CreatePragmaFunctionInfo;
struct CreateFunctionInfo;
struct CreateViewInfo;
struct CreateSequenceInfo;
struct CreateCollationInfo;
struct CreateIndexInfo;
struct CreateTypeInfo;
struct CreateTableInfo;
struct CreateGraphInfo;
struct CreatePartitionInfo;
struct CreatePropertySchemaInfo;
struct CreateExtentInfo;
struct CreateChunkDefinitionInfo;

class ClientContext;
class Transaction;

class AggregateFunctionCatalogEntry;
class CollateCatalogEntry;
class SchemaCatalogEntry;
class TableCatalogEntry;
class ViewCatalogEntry;
class SequenceCatalogEntry;
class TableFunctionCatalogEntry;
class CopyFunctionCatalogEntry;
class PragmaFunctionCatalogEntry;
class GraphCatalogEntry;
class PartitionCatalogEntry;
class PropertySchemaCatalogEntry;
class ExtentCatalogEntry;
class ChunkDefinitionCatalogEntry;
class CatalogSet;
class DatabaseInstance;
class DependencyManager;

//! Return value of Catalog::LookupEntry
struct CatalogEntryLookup {
	SchemaCatalogEntry *schema;
	CatalogEntry *entry;

	DUCKDB_API bool Found() const {
		return entry;
	}
};

//! Return value of SimilarEntryInSchemas
struct SimilarCatalogEntry {
	//! The entry name. Empty if absent
	string name;
	//! The distance to the given name.
	idx_t distance;
	//! The schema of the entry.
	SchemaCatalogEntry *schema;

	DUCKDB_API bool Found() const {
		return !name.empty();
	}

	DUCKDB_API string GetQualifiedName() const;
};

// Base ID (temporary)
#define LOGICAL_TYPE_BASE_ID 10000000L
#define EXPRESSION_TYPE_BASE_ID 20000000L
#define OPERATOR_BASE_ID 30000000L

//! The Catalog object represents the catalog of the database.
class Catalog {
	// typedef boost::interprocess::managed_shared_memory::segment_manager segment_manager_t;
	// typedef boost::interprocess::allocator<void, segment_manager_t> void_allocator;
	// typedef fixed_managed_mapped_file::const_named_iterator const_named_it;
	
public:
	explicit Catalog(DatabaseInstance &db);
	explicit Catalog(DatabaseInstance &db, fixed_managed_mapped_file *&catalog_segment);
	~Catalog();

	//! Reference to the database
	DatabaseInstance &db;
	//! The catalog set holding the schemas
	unique_ptr<CatalogSet> schemas;
	//! The DependencyManager manages dependencies between different catalog objects
	unique_ptr<DependencyManager> dependency_manager;
	//! Write lock for the catalog
	mutex write_lock;
	//! Shared memory manager
	fixed_managed_mapped_file *catalog_segment;

public:
	//! Get the ClientContext from the Catalog
	DUCKDB_API static Catalog &GetCatalog(ClientContext &context);
	DUCKDB_API static Catalog &GetCatalog(DatabaseInstance &db);
	DUCKDB_API void LoadCatalog(fixed_managed_mapped_file *&catalog_segment, vector<vector<string>> &object_names);

	DUCKDB_API DependencyManager &GetDependencyManager() {
		return *dependency_manager;
	}

	//! Returns the current version of the catalog (incremented whenever anything changes, not stored between restarts)
	DUCKDB_API idx_t GetCatalogVersion();
	//! Trigger a modification in the catalog, increasing the catalog version and returning the previous version
	DUCKDB_API idx_t ModifyCatalog();

	//! Creates a schema in the catalog.
	DUCKDB_API CatalogEntry *CreateSchema(ClientContext &context, CreateSchemaInfo *info);
	//! Creates a graph in the catalog.
	DUCKDB_API CatalogEntry *CreateGraph(ClientContext &context, CreateGraphInfo *info);
	//! Create a partition in the catalog
	DUCKDB_API CatalogEntry *CreatePartition(ClientContext &context, CreatePartitionInfo *info);
	//! Create a property schema in the catalog
	DUCKDB_API CatalogEntry *CreatePropertySchema(ClientContext &context, CreatePropertySchemaInfo *info);
	//! Create a extent in the catalog
	DUCKDB_API CatalogEntry *CreateExtent(ClientContext &context, CreateExtentInfo *info);
	//! Create a chunk definition in the catalog
	DUCKDB_API CatalogEntry *CreateChunkDefinition(ClientContext &context, CreateChunkDefinitionInfo *info);
	//! Create a scalar or aggregate function in the catalog
	DUCKDB_API CatalogEntry *CreateFunction(ClientContext &context, CreateFunctionInfo *info);
	//! Creates an index in the catalog
	DUCKDB_API CatalogEntry *CreateIndex(ClientContext &context, CreateIndexInfo *info);

	//! Creates a graph in the catalog.
	DUCKDB_API CatalogEntry *CreateGraph(ClientContext &context, SchemaCatalogEntry *schema,
	                                     CreateGraphInfo *info);
	//! Create a partition in the catalog
	DUCKDB_API CatalogEntry *CreatePartition(ClientContext &context, SchemaCatalogEntry *schema,
	                                     CreatePartitionInfo *info);
	//! Create a property schema in the catalog
	DUCKDB_API CatalogEntry *CreatePropertySchema(ClientContext &context, SchemaCatalogEntry *schema,
	                                     CreatePropertySchemaInfo *info);
	//! Create a extent in the catalog
	DUCKDB_API CatalogEntry *CreateExtent(ClientContext &context, SchemaCatalogEntry *schema,
	                                     CreateExtentInfo *info);
	//! Create a chunk definition in the catalog
	DUCKDB_API CatalogEntry *CreateChunkDefinition(ClientContext &context, SchemaCatalogEntry *schema,
	                                     CreateChunkDefinitionInfo *info);
	//! Create a scalar or aggregate function in the catalog
	DUCKDB_API CatalogEntry *CreateFunction(ClientContext &context, SchemaCatalogEntry *schema,
	                                        CreateFunctionInfo *info);
	//! Creates an index in the catalog
	DUCKDB_API CatalogEntry *CreateIndex(ClientContext &context, SchemaCatalogEntry *schema,
											CreateIndexInfo *info);

	/*
	//! Creates a table in the catalog.
	DUCKDB_API CatalogEntry *CreateTable(ClientContext &context, BoundCreateTableInfo *info);
	//! Creates a table in the catalog.
	DUCKDB_API CatalogEntry *CreateTable(ClientContext &context, unique_ptr<CreateTableInfo> info);
	//! Create a table function in the catalog
	DUCKDB_API CatalogEntry *CreateTableFunction(ClientContext &context, CreateTableFunctionInfo *info);
	//! Create a copy function in the catalog
	DUCKDB_API CatalogEntry *CreateCopyFunction(ClientContext &context, CreateCopyFunctionInfo *info);
	//! Create a pragma function in the catalog
	DUCKDB_API CatalogEntry *CreatePragmaFunction(ClientContext &context, CreatePragmaFunctionInfo *info);
	//! Creates a table in the catalog.
	DUCKDB_API CatalogEntry *CreateView(ClientContext &context, CreateViewInfo *info);
	//! Creates a sequence in the catalog.
	DUCKDB_API CatalogEntry *CreateSequence(ClientContext &context, CreateSequenceInfo *info);
	//! Creates a Enum in the catalog.
	DUCKDB_API CatalogEntry *CreateType(ClientContext &context, CreateTypeInfo *info);
	//! Creates a collation in the catalog
	DUCKDB_API CatalogEntry *CreateCollation(ClientContext &context, CreateCollationInfo *info);

	//! Creates a table in the catalog.
	DUCKDB_API CatalogEntry *CreateTable(ClientContext &context, SchemaCatalogEntry *schema,
	                                     BoundCreateTableInfo *info);
	//! Create a table function in the catalog
	DUCKDB_API CatalogEntry *CreateTableFunction(ClientContext &context, SchemaCatalogEntry *schema,
	                                             CreateTableFunctionInfo *info);
	//! Create a copy function in the catalog
	DUCKDB_API CatalogEntry *CreateCopyFunction(ClientContext &context, SchemaCatalogEntry *schema,
	                                            CreateCopyFunctionInfo *info);
	//! Create a pragma function in the catalog
	DUCKDB_API CatalogEntry *CreatePragmaFunction(ClientContext &context, SchemaCatalogEntry *schema,
	                                              CreatePragmaFunctionInfo *info);
	//! Creates a table in the catalog.
	DUCKDB_API CatalogEntry *CreateView(ClientContext &context, SchemaCatalogEntry *schema, CreateViewInfo *info);
	//! Creates a table in the catalog.
	DUCKDB_API CatalogEntry *CreateSequence(ClientContext &context, SchemaCatalogEntry *schema,
	                                        CreateSequenceInfo *info);
	//! Creates a enum in the catalog.
	DUCKDB_API CatalogEntry *CreateType(ClientContext &context, SchemaCatalogEntry *schema, CreateTypeInfo *info);
	//! Creates a collation in the catalog
	DUCKDB_API CatalogEntry *CreateCollation(ClientContext &context, SchemaCatalogEntry *schema,
	                                         CreateCollationInfo *info);
	*/

	//! Drops an entry from the catalog
	DUCKDB_API void DropEntry(ClientContext &context, DropInfo *info);

	//! Returns the schema object with the specified name, or throws an exception if it does not exist
	DUCKDB_API SchemaCatalogEntry *GetSchema(ClientContext &context, const string &name = DEFAULT_SCHEMA,
	                                         bool if_exists = false);
	                                         //QueryErrorContext error_context = QueryErrorContext());
	//! Scans all the schemas in the system one-by-one, invoking the callback for each entry
	DUCKDB_API void ScanSchemas(ClientContext &context, std::function<void(CatalogEntry *)> callback);
	//! Gets the "schema.name" entry of the specified type, if if_exists=true returns nullptr if entry does not exist,
	//! otherwise an exception is thrown
	DUCKDB_API CatalogEntry *GetEntry(ClientContext &context, CatalogType type, const string &schema,
	                                  const string &name, bool if_exists = false);
	                                  //QueryErrorContext error_context = QueryErrorContext());
	DUCKDB_API CatalogEntry *GetEntry(ClientContext &context, const string &schema, idx_t oid, bool if_exists = false);

	//! Gets the "schema.name" entry without a specified type, if entry does not exist an exception is thrown
	DUCKDB_API CatalogEntry *GetEntry(ClientContext &context, const string &schema, const string &name);

	template <class T>
	T *GetEntry(ClientContext &context, const string &schema_name, const string &name, bool if_exists = false);
	            //QueryErrorContext error_context = QueryErrorContext());

	//! Append a scalar or aggregate function to the catalog
	DUCKDB_API CatalogEntry *AddFunction(ClientContext &context, CreateFunctionInfo *info);
	//! Append a scalar or aggregate function to the catalog
	DUCKDB_API CatalogEntry *AddFunction(ClientContext &context, SchemaCatalogEntry *schema, CreateFunctionInfo *info);

	//! Alter an existing entry in the catalog.
	DUCKDB_API void Alter(ClientContext &context, AlterInfo *info);

private:
	//! The catalog version, incremented whenever anything changes in the catalog
	atomic<idx_t> catalog_version;

private:
	//! A variation of GetEntry that returns an associated schema as well.
	CatalogEntryLookup LookupEntry(ClientContext &context, CatalogType type, const string &schema, const string &name,
	                               bool if_exists = false);//, QueryErrorContext error_context = QueryErrorContext());

	//! Return an exception with did-you-mean suggestion.
	//CatalogException CreateMissingEntryException(ClientContext &context, const string &entry_name, CatalogType type,
	//                                             const vector<SchemaCatalogEntry *> &schemas);
	                                             //QueryErrorContext error_context);

	//! Return the close entry name, the distance and the belonging schema.
	SimilarCatalogEntry SimilarEntryInSchemas(ClientContext &context, const string &entry_name, CatalogType type,
	                                          const vector<SchemaCatalogEntry *> &schemas);

	void DropSchema(ClientContext &context, DropInfo *info);
};

template <>
DUCKDB_API GraphCatalogEntry *Catalog::GetEntry(ClientContext &context, const string &schema_name, const string &name,
                                                bool if_exists);//, QueryErrorContext error_context);
template <>
DUCKDB_API PartitionCatalogEntry *Catalog::GetEntry(ClientContext &context, const string &schema_name,
                                                   const string &name, bool if_exists);//, QueryErrorContext error_context);
template <>
DUCKDB_API PropertySchemaCatalogEntry *Catalog::GetEntry(ClientContext &context, const string &schema_name,
                                                   const string &name, bool if_exists);//, QueryErrorContext error_context);
template <>
DUCKDB_API ExtentCatalogEntry *Catalog::GetEntry(ClientContext &context, const string &schema_name,
                                                   const string &name, bool if_exists);//, QueryErrorContext error_context);
template <>
DUCKDB_API ChunkDefinitionCatalogEntry *Catalog::GetEntry(ClientContext &context, const string &schema_name,
                                                   const string &name, bool if_exists);//, QueryErrorContext error_context);

/*
template <>
DUCKDB_API TableCatalogEntry *Catalog::GetEntry(ClientContext &context, const string &schema_name, const string &name,
                                                bool if_exists);//, QueryErrorContext error_context);
template <>
DUCKDB_API SequenceCatalogEntry *Catalog::GetEntry(ClientContext &context, const string &schema_name,
                                                   const string &name, bool if_exists);//, QueryErrorContext error_context);
template <>
DUCKDB_API TableFunctionCatalogEntry *Catalog::GetEntry(ClientContext &context, const string &schema_name,
                                                        const string &name, bool if_exists);
                                                        //QueryErrorContext error_context);
template <>
DUCKDB_API CopyFunctionCatalogEntry *Catalog::GetEntry(ClientContext &context, const string &schema_name,
                                                       const string &name, bool if_exists);
                                                       //QueryErrorContext error_context);
template <>
DUCKDB_API PragmaFunctionCatalogEntry *Catalog::GetEntry(ClientContext &context, const string &schema_name,
                                                         const string &name, bool if_exists);
                                                         //QueryErrorContext error_context);
template <>
DUCKDB_API AggregateFunctionCatalogEntry *Catalog::GetEntry(ClientContext &context, const string &schema_name,
                                                            const string &name, bool if_exists);
                                                            //QueryErrorContext error_context);
template <>
DUCKDB_API CollateCatalogEntry *Catalog::GetEntry(ClientContext &context, const string &schema_name, const string &name,
                                                  bool if_exists);//, QueryErrorContext error_context);
*/

} // namespace duckdb
