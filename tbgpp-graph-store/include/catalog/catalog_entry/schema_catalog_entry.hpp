//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/schema_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/catalog_entry.hpp"
#include "catalog/catalog_set.hpp"
//#include "parser/query_error_context.hpp"

namespace duckdb {
class ClientContext;

class StandardEntry;
class TableCatalogEntry;
class TableFunctionCatalogEntry;
class SequenceCatalogEntry;
class GraphCatalogEntry;
class PartitionCatalogEntry;
class PropertySchemaCatalogEntry;
class ExtentCatalogEntry;
class ChunkDefinitionCatalogEntry;
class Serializer;
class Deserializer;

enum class OnCreateConflict : uint8_t;

struct AlterTableInfo;
struct CreateIndexInfo;
struct CreateFunctionInfo;
struct CreateCollationInfo;
struct CreateViewInfo;
struct BoundCreateTableInfo;
struct CreatePragmaFunctionInfo;
struct CreateSequenceInfo;
struct CreateSchemaInfo;
struct CreateTableFunctionInfo;
struct CreateCopyFunctionInfo;
struct CreateTypeInfo;
struct CreateGraphInfo;
struct CreatePartitionInfo;
struct CreatePropertySchemaInfo;
struct CreateExtentInfo;
struct CreateChunkDefinitionInfo;

struct DropInfo;

//! A schema in the catalog
class SchemaCatalogEntry : public CatalogEntry {
	typedef boost::interprocess::managed_unique_ptr<CatalogEntry, boost::interprocess::managed_shared_memory>::type unique_ptr_type;
	typedef boost::interprocess::managed_unique_ptr<GraphCatalogEntry, boost::interprocess::managed_shared_memory>::type graph_unique_ptr_type;
	typedef boost::interprocess::managed_unique_ptr<PartitionCatalogEntry, boost::interprocess::managed_shared_memory>::type partition_unique_ptr_type;
	typedef boost::interprocess::managed_unique_ptr<PropertySchemaCatalogEntry, boost::interprocess::managed_shared_memory>::type propertyschema_unique_ptr_type;
	typedef boost::interprocess::managed_unique_ptr<ExtentCatalogEntry, boost::interprocess::managed_shared_memory>::type extent_unique_ptr_type;
	typedef boost::interprocess::managed_unique_ptr<ChunkDefinitionCatalogEntry, boost::interprocess::managed_shared_memory>::type chunkdefinition_unique_ptr_type;
	friend class Catalog;

public:
	SchemaCatalogEntry(Catalog *catalog, string name, bool is_internal);
	SchemaCatalogEntry(Catalog *catalog, string name, bool is_internal, boost::interprocess::managed_shared_memory *&catalog_segment);

private:
	//! The catalog set holding the graphs
	CatalogSet graphs;
	//! The catalog set holding the partitions
	CatalogSet partitions;
	//! The catalog set holding the propertyschemas
	CatalogSet propertyschemas;
	//! The catalog set holding the extents
	CatalogSet extents;
	//! The catalog set holding the chunkdefinitions
	CatalogSet chunkdefinitions;
	/*
	//! The catalog set holding the tables
	CatalogSet tables;
	//! The catalog set holding the indexes
	CatalogSet indexes;
	//! The catalog set holding the table functions
	CatalogSet table_functions;
	//! The catalog set holding the copy functions
	CatalogSet copy_functions;
	//! The catalog set holding the pragma functions
	CatalogSet pragma_functions;
	//! The catalog set holding the scalar and aggregate functions
	CatalogSet functions;
	//! The catalog set holding the sequences
	CatalogSet sequences;
	//! The catalog set holding the collations
	CatalogSet collations;
	//! The catalog set holding the types
	CatalogSet types;
	*/
	boost::interprocess::managed_shared_memory *catalog_segment;

public:
	//! Scan the specified catalog set, invoking the callback method for every entry
	void Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry *)> &callback);
	//! Scan the specified catalog set, invoking the callback method for every committed entry
	void Scan(CatalogType type, const std::function<void(CatalogEntry *)> &callback);

	//! Serialize the meta information of the SchemaCatalogEntry a serializer
	virtual void Serialize(Serializer &serializer);
	//! Deserializes to a CreateSchemaInfo
	static unique_ptr<CreateSchemaInfo> Deserialize(Deserializer &source);

	string ToSQL() override;

	//! Creates an index with the given name in the schema
	//CatalogEntry *CreateIndex(ClientContext &context, CreateIndexInfo *info, TableCatalogEntry *table);

private:
	//! Creates a graph with the given name in the schema
	CatalogEntry *CreateGraph(ClientContext &context, CreateGraphInfo *info);
	//! Creates a partition with the given name in the schema
	CatalogEntry *CreatePartition(ClientContext &context, CreatePartitionInfo *info);
	//! Creates a property schema with the given name in the schema
	CatalogEntry *CreatePropertySchema(ClientContext &context, CreatePropertySchemaInfo *info);
	//! Creates a extent with the given name in the schema
	CatalogEntry *CreateExtent(ClientContext &context, CreateExtentInfo *info);
	//! Creates a chunk definition with the given name in the schema
	CatalogEntry *CreateChunkDefinition(ClientContext &context, CreateChunkDefinitionInfo *info);
	/*
	//! Create a scalar or aggregate function within the given schema
	CatalogEntry *CreateFunction(ClientContext &context, CreateFunctionInfo *info);
	//! Creates a table with the given name in the schema
	CatalogEntry *CreateTable(ClientContext &context, BoundCreateTableInfo *info);
	//! Creates a view with the given name in the schema
	CatalogEntry *CreateView(ClientContext &context, CreateViewInfo *info);
	//! Creates a sequence with the given name in the schema
	CatalogEntry *CreateSequence(ClientContext &context, CreateSequenceInfo *info);
	//! Create a table function within the given schema
	CatalogEntry *CreateTableFunction(ClientContext &context, CreateTableFunctionInfo *info);
	//! Create a copy function within the given schema
	CatalogEntry *CreateCopyFunction(ClientContext &context, CreateCopyFunctionInfo *info);
	//! Create a pragma function within the given schema
	CatalogEntry *CreatePragmaFunction(ClientContext &context, CreatePragmaFunctionInfo *info);
	//! Create a collation within the given schema
	CatalogEntry *CreateCollation(ClientContext &context, CreateCollationInfo *info);
	//! Create a enum within the given schema
	CatalogEntry *CreateType(ClientContext &context, CreateTypeInfo *info);
	*/

	//! Drops an entry from the schema
	void DropEntry(ClientContext &context, DropInfo *info);

	/*
	//! Append a scalar or aggregate function within the given schema
	CatalogEntry *AddFunction(ClientContext &context, CreateFunctionInfo *info);
	*/

	//! Alters a catalog entry
	void Alter(ClientContext &context, AlterInfo *info);

	//! Add a catalog entry to this schema
	CatalogEntry *AddEntry(ClientContext &context, unique_ptr<StandardEntry> entry, OnCreateConflict on_conflict);
	//! Add a catalog entry to this schema
	CatalogEntry *AddEntry(ClientContext &context, unique_ptr<StandardEntry> entry, OnCreateConflict on_conflict,
	                       unordered_set<CatalogEntry *> dependencies);
	GraphCatalogEntry *AddGraphEntry(ClientContext &context, graph_unique_ptr_type entry,
							OnCreateConflict on_conflict, unordered_set<CatalogEntry *> dependencies);
	PartitionCatalogEntry *AddPartitionEntry(ClientContext &context, partition_unique_ptr_type entry,
							OnCreateConflict on_conflict, unordered_set<CatalogEntry *> dependencies);
	PropertySchemaCatalogEntry *AddPropertySchemaEntry(ClientContext &context, propertyschema_unique_ptr_type entry,
							OnCreateConflict on_conflict, unordered_set<CatalogEntry *> dependencies);
	ExtentCatalogEntry *AddExtentEntry(ClientContext &context, extent_unique_ptr_type entry,
							OnCreateConflict on_conflict, unordered_set<CatalogEntry *> dependencies);
	ChunkDefinitionCatalogEntry *AddChunkDefinitionEntry(ClientContext &context, chunkdefinition_unique_ptr_type entry,
							OnCreateConflict on_conflict, unordered_set<CatalogEntry *> dependencies);


	//! Get the catalog set for the specified type
	CatalogSet &GetCatalogSet(CatalogType type);
};
} // namespace duckdb
