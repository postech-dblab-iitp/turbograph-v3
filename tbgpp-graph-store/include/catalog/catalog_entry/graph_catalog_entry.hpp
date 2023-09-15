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
#include "common/boost_typedefs.hpp"

namespace duckdb {

class ColumnStatistics;
struct CreateGraphInfo;
struct PartitionCatalogEntry;

//! A graph catalog entry
class GraphCatalogEntry : public StandardEntry {
	typedef boost::unordered_map< char_string, VertexLabelID
       	, boost::hash<char_string>, std::equal_to<char_string>
		, vertexlabel_id_map_value_type_allocator>
	VertexLabelIDUnorderedMap;
	typedef boost::unordered_map< char_string, EdgeTypeID
       	, boost::hash<char_string>, std::equal_to<char_string>
		, edgetype_id_map_value_type_allocator>
	EdgeTypeIDUnorderedMap;
	typedef boost::unordered_map< char_string, PropertyKeyID
       	, boost::hash<char_string>, std::equal_to<char_string>
		, propertykey_id_map_value_type_allocator>
	PropertyKeyIDUnorderedMap;
	typedef boost::unordered_map< EdgeTypeID, idx_t
       	, boost::hash<EdgeTypeID>, std::equal_to<EdgeTypeID>
		, type_to_partition_map_value_type_allocator>
	EdgeTypeToPartitionUnorderedMap;
	typedef boost::unordered_map< VertexLabelID, idx_t_vector
       	, boost::hash<VertexLabelID>, std::equal_to<VertexLabelID>
		, label_to_partitionvec_map_value_type_allocator>
	VertexLabelToPartitionVecUnorderedMap;

public:
	//! Create a real GraphCatalogEntry
	GraphCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateGraphInfo *info, const void_allocator &void_alloc);

	PartitionID_vector vertex_partitions;
	PartitionID_vector edge_partitions;

	// TODO: change map structure into.. what?
	VertexLabelIDUnorderedMap vertexlabel_map;
	EdgeTypeIDUnorderedMap edgetype_map;
	PropertyKeyIDUnorderedMap propertykey_map;

	//unordered_map<EdgeTypeID, PartitionID> type_to_partition_index; // multiple partitions for a edge type?
	EdgeTypeToPartitionUnorderedMap type_to_partition_index;
	VertexLabelToPartitionVecUnorderedMap label_to_partition_index;

	atomic<VertexLabelID> vertex_label_id_version;
	atomic<EdgeTypeID> edge_type_id_version;
	atomic<PropertyKeyID> property_key_id_version;
	atomic<PartitionID> partition_id_version;
public:
	//unique_ptr<CatalogEntry> AlterEntry(ClientContext &context, AlterInfo *info) override;
	void AddVertexPartition(ClientContext &context, PartitionID pid, idx_t oid, vector<VertexLabelID>& label_ids);
	void AddVertexPartition(ClientContext &context, PartitionID pid, idx_t oid, vector<string>& labels);
	void AddEdgePartition(ClientContext &context, PartitionID pid, idx_t oid, EdgeTypeID edge_type_id);
	void AddEdgePartition(ClientContext &context, PartitionID pid, idx_t oid, string type);

	vector<idx_t> LookupPartition(ClientContext &context, vector<string> keys, GraphComponentType graph_component_type);
	void GetPropertyKeyIDs(ClientContext &context, vector<string>& property_schemas, vector<PropertyKeyID>& property_key_ids);
	void GetVertexLabels(vector<string>& label_names);
	void GetEdgeTypes(vector<string>& type_names);
	void GetVertexPartitionIndexesInLabel(ClientContext &context, string label, vector<idx_t> &vertex_partition_indexes);
	void GetEdgePartitionIndexesInType(ClientContext &context, string type, vector<idx_t> &edge_partition_indexes);

	vector<idx_t> Intersection(ClientContext &context, vector<VertexLabelID>& label_ids);
	VertexLabelID GetVertexLabelID();
	EdgeTypeID GetEdgeTypeID();
	PropertyKeyID GetPropertyKeyID();
	PartitionID GetNewPartitionID();

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
