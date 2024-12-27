#pragma once

#include "common/common.hpp"
#include "common/boost.hpp"
#include "common/types.hpp"

namespace duckdb {
    #define MIN_MAX_ARRAY_SIZE 1024
    #define CATALOG_ADDR 0x10007fff8000

    struct minmax_t {
        int64_t min = 0;
        int64_t max = 0;
    };

    struct welford_t {
        idx_t n = 0;
        int64_t mean = 0;
        int64_t M2 = 0;
    };

    // typedefs for shared memory object
    typedef boost::interprocess::basic_managed_shared_memory< char,boost::interprocess::rbtree_best_fit< boost::interprocess::mutex_family, void * >,boost::interprocess::iset_index > fixed_managed_shared_memory;
    typedef boost::interprocess::basic_managed_mapped_file< char,boost::interprocess::rbtree_best_fit< boost::interprocess::mutex_family, boost::interprocess::offset_ptr<void> >,boost::interprocess::iset_index > fixed_managed_mapped_file;
	// typedef fixed_managed_shared_memory::segment_manager segment_manager_t;
    typedef fixed_managed_mapped_file::segment_manager segment_manager_t;
    typedef fixed_managed_mapped_file::const_named_iterator const_named_it;
	typedef boost::interprocess::allocator<void, segment_manager_t> void_allocator;
	typedef boost::interprocess::allocator<bool, segment_manager_t> bool_allocator;
	typedef boost::interprocess::allocator<idx_t, segment_manager_t> idx_t_allocator;
    typedef boost::interprocess::allocator<uint16_t, segment_manager_t> uint16_t_allocator;
    typedef boost::interprocess::allocator<int64_t, segment_manager_t> int64_t_allocator;
    typedef boost::interprocess::allocator<uint64_t, segment_manager_t> uint64_t_allocator;
    typedef boost::interprocess::allocator<void*, segment_manager_t> void_pointer_allocator;
	typedef boost::interprocess::allocator<char, segment_manager_t> char_allocator;
	typedef boost::interprocess::allocator<transaction_t, segment_manager_t> transaction_t_allocator;
    typedef boost::interprocess::allocator<PartitionID, segment_manager_t> partitionid_allocator;
    typedef boost::interprocess::allocator<PropertyKeyID, segment_manager_t> propertykeyid_allocator;
    typedef boost::interprocess::allocator<PropertySchemaID, segment_manager_t> propertyschemaid_allocator;
    typedef boost::interprocess::allocator<ChunkDefinitionID, segment_manager_t> chunkdefinitionid_allocator;
    typedef boost::interprocess::basic_string<char, std::char_traits<char>, char_allocator> char_string;
    typedef boost::interprocess::allocator<char_string, segment_manager_t> string_allocator;
    typedef boost::interprocess::allocator<LogicalTypeId, segment_manager_t> logicaltypeid_allocator;
    typedef boost::interprocess::vector<idx_t, idx_t_allocator> idx_t_vector;
    typedef boost::interprocess::vector<uint16_t, uint16_t_allocator> uint16_t_vector;
    typedef boost::interprocess::vector<int64_t, int64_t_allocator> int64_t_vector;
    typedef boost::interprocess::vector<uint64_t, uint64_t_allocator> uint64_t_vector;
    typedef boost::interprocess::vector<void*, void_pointer_allocator> void_pointer_vector;
    typedef boost::interprocess::vector<char_string, string_allocator> string_vector;
    typedef boost::interprocess::vector<PartitionID, partitionid_allocator> PartitionID_vector;
    typedef boost::interprocess::vector<PropertyKeyID, propertykeyid_allocator> PropertyKeyID_vector;
    typedef boost::interprocess::vector<PropertySchemaID, propertyschemaid_allocator> PropertySchemaID_vector;
    typedef boost::interprocess::vector<ChunkDefinitionID, chunkdefinitionid_allocator> ChunkDefinitionID_vector;
    typedef boost::interprocess::vector<LogicalTypeId, logicaltypeid_allocator> LogicalTypeId_vector;
    typedef std::pair<const char_string, VertexLabelID> vertexlabel_id_map_value_type;
    typedef std::pair<const char_string, EdgeTypeID> edgetype_id_map_value_type;
    typedef std::pair<const char_string, PropertyKeyID> propertykey_id_map_value_type;
    typedef std::pair<idx_t, void*> idx_t_to_void_ptr_value_type;
    typedef std::pair<idx_t, idx_t> idx_t_to_idx_t_value_type;
    typedef std::pair<EdgeTypeID, idx_t> type_to_partition_map_value_type;
    typedef std::pair<VertexLabelID, idx_t_vector> label_to_partitionvec_map_value_type;
    typedef std::pair<PropertyKeyID, PropertySchemaID_vector> property_to_propertyschemavec_map_value_type;
    typedef std::pair<idx_t, idx_t_vector> idx_t_to_idx_t_vec_map_value_type;
    typedef std::pair<PartitionID, PartitionID_vector> partition_to_partitionvec_map_value_type;
    typedef boost::interprocess::allocator<vertexlabel_id_map_value_type, segment_manager_t> vertexlabel_id_map_value_type_allocator;
    typedef boost::interprocess::allocator<edgetype_id_map_value_type, segment_manager_t> edgetype_id_map_value_type_allocator;
    typedef boost::interprocess::allocator<idx_t_to_void_ptr_value_type, segment_manager_t> idx_t_to_void_ptr_value_type_allocator;
    typedef boost::interprocess::allocator<idx_t_to_idx_t_value_type, segment_manager_t> idx_t_to_idx_t_value_type_allocator;
    typedef boost::interprocess::allocator<propertykey_id_map_value_type, segment_manager_t> propertykey_id_map_value_type_allocator;
    typedef boost::interprocess::allocator<type_to_partition_map_value_type, segment_manager_t> type_to_partition_map_value_type_allocator;
    typedef boost::interprocess::allocator<label_to_partitionvec_map_value_type, segment_manager_t> label_to_partitionvec_map_value_type_allocator;
    typedef boost::interprocess::allocator<property_to_propertyschemavec_map_value_type, segment_manager_t> property_to_propertyschemavec_map_value_type_allocator;
    typedef boost::interprocess::allocator<partition_to_partitionvec_map_value_type, segment_manager_t> partition_to_partitionvec_map_value_type_allocator;
    typedef boost::interprocess::allocator<idx_t_to_idx_t_vec_map_value_type, segment_manager_t> idx_t_to_idx_t_vec_map_value_type_allocator;
    typedef boost::interprocess::vector<idx_t_to_idx_t_value_type, idx_t_to_idx_t_value_type_allocator> idx_t_pair_vector;
    typedef std::pair<PropertyKeyID, idx_t_pair_vector> property_to_idx_t_pairvec_map_value_type;
    typedef boost::interprocess::allocator<property_to_idx_t_pairvec_map_value_type, segment_manager_t> property_to_idx_t_pairvec_map_value_type_allocator;
    typedef boost::unordered_map< PropertyKeyID, PropertySchemaID_vector
       	, boost::hash<PropertyKeyID>, std::equal_to<PropertyKeyID>
		, property_to_propertyschemavec_map_value_type_allocator>
	PropertyToPropertySchemaVecUnorderedMap;
    typedef boost::unordered_map< PropertyKeyID, idx_t_pair_vector
       	, boost::hash<PropertyKeyID>, std::equal_to<PropertyKeyID>
		, property_to_idx_t_pairvec_map_value_type_allocator>
	PropertyToPropertySchemaPairVecUnorderedMap;
    typedef boost::unordered_map< PropertyKeyID, idx_t
       	, boost::hash<PropertyKeyID>, std::equal_to<PropertyKeyID>
		, idx_t_to_idx_t_value_type_allocator>
	PropertyToIdxUnorderedMap;
    typedef boost::unordered_map< char_string, idx_t
       	, boost::hash<char_string>, std::equal_to<char_string>
        , string_allocator>
    PropertyNameToColumnIdxUnorderedMap;
	typedef boost::interprocess::allocator<minmax_t, segment_manager_t> minmax_allocator;
	typedef boost::interprocess::vector<minmax_t, minmax_allocator> minmax_t_vector;
    typedef boost::interprocess::allocator<welford_t, segment_manager_t> welford_allocator;
    typedef boost::interprocess::vector<welford_t, welford_allocator> welford_t_vector;
} // namespace duckdb
