#pragma once

#include "catalog/catalog_entry.hpp"
#include "catalog/catalog_set.hpp"

namespace s62 {
class ClientContext;

class StandardEntry;
class TableCatalogEntry;
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
struct CreateViewInfo;
struct BoundCreateTableInfo;
struct CreateSequenceInfo;
struct CreateSchemaInfo;
struct CreateGraphInfo;
struct CreatePartitionInfo;
struct CreatePropertySchemaInfo;
struct CreateExtentInfo;
struct CreateChunkDefinitionInfo;

struct DropInfo;

//! A schema in the catalog
class SchemaCatalogEntry : public CatalogEntry {
	typedef boost::unordered_map< idx_t, void*
       	, boost::hash<idx_t>, std::equal_to<idx_t>
		, idx_t_to_void_ptr_value_type_allocator>
	OidToCatalogEntryPtrUnorderedMap;
	// maybe useless typedefs.. TODO
	typedef boost::interprocess::allocator<void, segment_manager_t> void_allocator;
	typedef boost::interprocess::managed_unique_ptr<CatalogEntry, fixed_managed_mapped_file>::type unique_ptr_type;
	typedef boost::interprocess::managed_unique_ptr<GraphCatalogEntry, fixed_managed_mapped_file>::type graph_unique_ptr_type;
	typedef boost::interprocess::managed_unique_ptr<PartitionCatalogEntry, fixed_managed_mapped_file>::type partition_unique_ptr_type;
	typedef boost::interprocess::managed_unique_ptr<PropertySchemaCatalogEntry, fixed_managed_mapped_file>::type propertyschema_unique_ptr_type;
	typedef boost::interprocess::managed_unique_ptr<ExtentCatalogEntry, fixed_managed_mapped_file>::type extent_unique_ptr_type;
	typedef boost::interprocess::managed_unique_ptr<ChunkDefinitionCatalogEntry, fixed_managed_mapped_file>::type chunkdefinition_unique_ptr_type;
	friend class Catalog;

public:
	// SchemaCatalogEntry(Catalog *catalog, string name, bool is_internal);
	SchemaCatalogEntry(Catalog *catalog, string name, bool is_internal, fixed_managed_mapped_file *&catalog_segment);

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
	//! The catalog set holding the indexes
	CatalogSet indexes;
	//! The catalog set holding the scalar and aggregate functions
	CatalogSet functions;

	fixed_managed_mapped_file *catalog_segment;
	//! oid to catalog entry array
	OidToCatalogEntryPtrUnorderedMap oid_to_catalog_entry_array;

public:
	//! Scan the specified catalog set, invoking the callback method for every entry
	void Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry *)> &callback);
	//! Scan the specified catalog set, invoking the callback method for every committed entry
	void Scan(CatalogType type, const std::function<void(CatalogEntry *)> &callback);

	//! Serialize the meta information of the SchemaCatalogEntry a serializer
	virtual void Serialize(Serializer &serializer);
	//! Deserializes to a CreateSchemaInfo
	static unique_ptr<CreateSchemaInfo> Deserialize(Deserializer &source);
	void LoadCatalogSet();
	void SetCatalogSegment(fixed_managed_mapped_file *&catalog_segment);

	string ToSQL() override;

	CatalogEntry *GetCatalogEntryFromOid(idx_t oid);

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
	//! Create a scalar or aggregate function within the given schema
	CatalogEntry *CreateFunction(ClientContext &context, CreateFunctionInfo *info);
	//! Creates an index with the given name in the schema
	CatalogEntry *CreateIndex(ClientContext &context, CreateIndexInfo *info);

	//! Drops an entry from the schema
	void DropEntry(ClientContext &context, DropInfo *info);

	//! Append a scalar or aggregate function within the given schema
	CatalogEntry *AddFunction(ClientContext &context, CreateFunctionInfo *info);

	//! Alters a catalog entry
	void Alter(ClientContext &context, AlterInfo *info);

	//! Add a catalog entry to this schema
	CatalogEntry *AddEntry(ClientContext &context, StandardEntry *entry, OnCreateConflict on_conflict);
	//! Add a catalog entry to this schema
	CatalogEntry *AddEntry(ClientContext &context, StandardEntry *entry, OnCreateConflict on_conflict,
	                       unordered_set<CatalogEntry *> dependencies);
						   
	bool AddEntryInternal(ClientContext &context, CatalogEntry *entry, string &entry_name, CatalogType &entry_type,
                                           OnCreateConflict on_conflict, unordered_set<CatalogEntry *> dependencies);

	//! Get the catalog set for the specified type
	CatalogSet &GetCatalogSet(CatalogType type);
};
} // namespace s62
