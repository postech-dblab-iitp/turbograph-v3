//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/table_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once


#include "catalog/standard_entry.hpp"

#include "common/unordered_map.hpp"
#include "common/enums/extent_type.hpp"
//#include "parser/column_definition.hpp"
//#include "parser/constraint.hpp"
//#include "planner/bound_constraint.hpp"
//#include "planner/expression.hpp"
#include "common/case_insensitive_map.hpp"

namespace s62 {

class ColumnStatistics;
class DataTable;
struct CreateExtentInfo;
struct BoundCreateTableInfo;

struct RenameColumnInfo;
struct AddColumnInfo;
struct RemoveColumnInfo;
struct SetDefaultInfo;
struct ChangeColumnTypeInfo;
struct AlterForeignKeyInfo;

//! A extent catalog entry
class ExtentCatalogEntry : public StandardEntry {
public:
	//! Create a real GraphCatalogEntry
	ExtentCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateExtentInfo *info, const void_allocator &void_alloc);

	ExtentID eid;
	ExtentType extent_type;
	PartitionID pid; // foreign key
	idx_t ps_oid;
	// For properties
	ChunkDefinitionID_vector chunks;
	atomic<LocalChunkDefinitionID> local_cdf_id_version; // forward-growing
	// For adjlists
	ChunkDefinitionID_vector adjlist_chunks;
	atomic<LocalChunkDefinitionID> local_adjlist_cdf_id_version; // backward-growing
	size_t num_tuples_in_extent = 0;
	
public:
	//unique_ptr<CatalogEntry> AlterEntry(ClientContext &context, AlterInfo *info) override;
	
	//! Returns a list of types of the table

	//! Serialize the meta information of the TableCatalogEntry a serializer
	//virtual void Serialize(Serializer &serializer);
	//! Deserializes to a CreateTableInfo
	//static unique_ptr<CreateTableInfo> Deserialize(Deserializer &source);

	unique_ptr<CatalogEntry> Copy(ClientContext &context) override;

	void SetExtentType(ExtentType extent_type_);
	LocalChunkDefinitionID GetNextChunkDefinitionID();
	void AddChunkDefinitionID(ChunkDefinitionID cdf_id);
	LocalChunkDefinitionID GetNextAdjListChunkDefinitionID();
	void AddAdjListChunkDefinitionID(ChunkDefinitionID cdf_id);

	size_t GetNumTuplesInExtent() {
		return num_tuples_in_extent;
	}
};
} // namespace s62
