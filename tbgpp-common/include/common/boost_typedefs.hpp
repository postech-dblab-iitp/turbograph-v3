#pragma once

#include "common/common.hpp"

namespace duckdb {
    // typedefs for shared memory object
    typedef boost::interprocess::basic_managed_shared_memory< char,boost::interprocess::rbtree_best_fit< boost::interprocess::mutex_family, void * >,boost::interprocess::iset_index > fixed_managed_shared_memory;
    typedef boost::interprocess::basic_managed_mapped_file< char,boost::interprocess::rbtree_best_fit< boost::interprocess::mutex_family, boost::interprocess::offset_ptr<void> >,boost::interprocess::iset_index > fixed_managed_mapped_file;
	// typedef fixed_managed_shared_memory::segment_manager segment_manager_t;
    typedef fixed_managed_mapped_file::segment_manager segment_manager_t;
    typedef fixed_managed_mapped_file::const_named_iterator const_named_it;
	typedef boost::interprocess::allocator<void, segment_manager_t> void_allocator;
	typedef boost::interprocess::allocator<bool, segment_manager_t> bool_allocator;
	typedef boost::interprocess::allocator<idx_t, segment_manager_t> idx_t_allocator;
	typedef boost::interprocess::allocator<char, segment_manager_t> char_allocator;
	typedef boost::interprocess::allocator<transaction_t, segment_manager_t> transaction_t_allocator;
    typedef boost::interprocess::allocator<PartitionID, segment_manager_t> partitionid_allocator;
    typedef boost::interprocess::allocator<PropertyKeyID, segment_manager_t> propertykeyid_allocator;
    typedef boost::interprocess::allocator<PropertySchemaID, segment_manager_t> propertyschemaid_allocator;
    typedef boost::interprocess::allocator<ChunkDefinitionID, segment_manager_t> chunkdefinitionid_allocator;
    typedef boost::interprocess::basic_string<char, std::char_traits<char>, char_allocator> char_string;
    typedef boost::interprocess::vector<idx_t, idx_t_allocator> idx_t_vector;
    typedef boost::interprocess::vector<PartitionID, partitionid_allocator> PartitionID_vector;
    typedef boost::interprocess::vector<PropertyKeyID, propertykeyid_allocator> PropertyKeyID_vector;
    typedef boost::interprocess::vector<PropertySchemaID, propertyschemaid_allocator> PropertySchemaID_vector;
    typedef boost::interprocess::vector<ChunkDefinitionID, chunkdefinitionid_allocator> ChunkDefinitionID_vector;
    typedef std::pair<const char_string, VertexLabelID> vertexlabel_id_map_value_type;
    typedef std::pair<const char_string, EdgeTypeID> edgetype_id_map_value_type;
    typedef std::pair<const char_string, PropertyKeyID> propertykey_id_map_value_type;
    typedef std::pair<EdgeTypeID, PartitionID> type_to_partition_map_value_type;
    typedef std::pair<VertexLabelID, PartitionID_vector> label_to_partitionvec_map_value_type;
    typedef std::pair<PropertyKeyID, PropertySchemaID_vector> property_to_propertyschemavec_map_value_type;
    typedef boost::interprocess::allocator<vertexlabel_id_map_value_type, segment_manager_t> vertexlabel_id_map_value_type_allocator;
    typedef boost::interprocess::allocator<edgetype_id_map_value_type, segment_manager_t> edgetype_id_map_value_type_allocator;
    typedef boost::interprocess::allocator<propertykey_id_map_value_type, segment_manager_t> propertykey_id_map_value_type_allocator;
    typedef boost::interprocess::allocator<type_to_partition_map_value_type, segment_manager_t> type_to_partition_map_value_type_allocator;
    typedef boost::interprocess::allocator<label_to_partitionvec_map_value_type, segment_manager_t> label_to_partitionvec_map_value_type_allocator;
    typedef boost::interprocess::allocator<property_to_propertyschemavec_map_value_type, segment_manager_t> property_to_propertyschemavec_map_value_type_allocator;
} // namespace duckdb
