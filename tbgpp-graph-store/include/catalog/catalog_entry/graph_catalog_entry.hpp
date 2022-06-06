#pragma once


#include "catalog/standard_entry.hpp"

#include "common/unordered_map.hpp"
#include "parser/column_definition.hpp"
//#include "parser/constraint.hpp"
//#include "planner/bound_constraint.hpp"
//#include "planner/expression.hpp"
#include "common/case_insensitive_map.hpp"
#include "common/enums/graph_component_type.hpp"
#include "catalog/inverted_index.hpp"

namespace duckdb {

class ColumnStatistics;
struct CreateGraphInfo;
struct PartitionCatalogEntry;

//! A graph catalog entry
class GraphCatalogEntry : public StandardEntry {
public:
	//! Create a real GraphCatalogEntry
	GraphCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateGraphInfo *info);

	vector<PartitionID> vertex_partitions;
	vector<PartitionID> edge_partitions;

	// TODO: change map structure into.. what?
	unordered_map<string, VertexLabelID> vertexlabel_map;
	unordered_map<string, EdgeTypeID> edgetype_map;
	unordered_map<string, PropertyKeyID> propertykey_map;

	unordered_map<EdgeTypeID, PartitionID> type_to_partition_index; // multiple partitions for a edge type?
	inverted_index_t<VertexLabelID, PartitionID> label_to_partition_index;

	atomic<VertexLabelID> vertex_label_id_version;
	atomic<EdgeTypeID> edge_type_id_version;
	atomic<PropertyKeyID> property_key_id_version;
public:
	//unique_ptr<CatalogEntry> AlterEntry(ClientContext &context, AlterInfo *info) override;
	void AddVertexPartition(ClientContext &context, PartitionID pid, vector<VertexLabelID>& label_ids);
	void AddVertexPartition(ClientContext &context, PartitionID pid, vector<string>& labels);
	void AddEdgePartition(ClientContext &context, PartitionID pid, EdgeTypeID edge_type_id);
	void AddEdgePartition(ClientContext &context, PartitionID pid, string type);

	PartitionID LookupPartition(ClientContext &context, vector<string> keys, GraphComponentType graph_component_type);
	void GetPropertyKeyIDs(ClientContext &context, vector<string>& property_schemas, vector<PropertyKeyID>& property_key_ids);

	vector<PartitionID> Intersection(vector<VertexLabelID>& label_ids);
	VertexLabelID GetVertexLabelID();
	EdgeTypeID GetEdgeTypeID();
	PropertyKeyID GetPropertyKeyID();

	//! Serialize the meta information of the TableCatalogEntry a serializer
	//virtual void Serialize(Serializer &serializer);
	//! Deserializes to a CreateTableInfo
	//static unique_ptr<CreateTableInfo> Deserialize(Deserializer &source);

	unique_ptr<CatalogEntry> Copy(ClientContext &context) override;

	//void CommitAlter(AlterInfo &info);
	//void CommitDrop();

	//! Returns the column index of the specified column name.
	//! If the column does not exist:
	//! If if_exists is true, returns DConstants::INVALID_INDEX
	//! If if_exists is false, throws an exception
	//idx_t GetColumnIndex(string &name, bool if_exists = false);

};
} // namespace duckdb
