#include "main/database.hpp"
#include "main/client_context.hpp"
#include "catalog/catalog.hpp"
#include "catalog/catalog_entry/list.hpp"
#include "common/enums/graph_component_type.hpp"
#include "parser/parsed_data/create_graph_info.hpp"
#include "cache/disk_aio/TypeDef.hpp"
#include "common/directory_helper.hpp"

#include "icecream.hpp"

#include <memory>
#include <algorithm>

namespace duckdb {

GraphCatalogEntry::GraphCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateGraphInfo *info, const void_allocator &void_alloc)
    : StandardEntry(CatalogType::GRAPH_ENTRY, schema, catalog, info->graph, void_alloc),
	vertex_partitions(void_alloc), edge_partitions(void_alloc), vertexlabel_map(void_alloc),
	edgetype_map(void_alloc), propertykey_map(void_alloc), type_to_partition_index(void_alloc),
	label_to_partition_index(void_alloc), src_part_to_connected_edge_part_index(void_alloc),
	propertykey_to_typeid_map(void_alloc), property_key_id_to_name_vec(void_alloc)
{
	this->temporary = info->temporary;
	vertex_label_id_version = 0;
	edge_type_id_version = 0;
	property_key_id_version = 1; // reserve 0 for '_id'
	partition_id_version = 0;
}

unique_ptr<CatalogEntry> GraphCatalogEntry::Copy(ClientContext &context) {
	D_ASSERT(false);
}
void GraphCatalogEntry::AddEdgePartition(ClientContext &context, PartitionID pid, idx_t oid, EdgeTypeID edge_type_id) {
	auto target_id = type_to_partition_index.find(edge_type_id);
	if (target_id != type_to_partition_index.end()) {
		// found ?
		D_ASSERT(false);
	} else {
		// not found
		type_to_partition_index.insert({edge_type_id, oid});
	}
	edge_partitions.push_back(oid);
	string partition_dir_path = DiskAioParameters::WORKSPACE + "/part_" + std::to_string(pid);
	MkDir(partition_dir_path, true);
}

void GraphCatalogEntry::AddEdgePartition(ClientContext &context, PartitionID pid, idx_t oid, string type) {
	char_allocator temp_charallocator (context.db->GetCatalog().catalog_segment->get_segment_manager());
	char_string type_(temp_charallocator);
	type_ = type.c_str();
	EdgeTypeID edge_type_id;
	auto type_id = edgetype_map.find(type_);
	if (type_id != edgetype_map.end()) {
		// label found in label map
		edge_type_id = type_id->second;
	} else {
		edge_type_id = GetEdgeTypeID();
		edgetype_map.insert({type_, edge_type_id});
	}
	auto target_id = type_to_partition_index.find(edge_type_id);
	if (target_id != type_to_partition_index.end()) {
		// found ?
		D_ASSERT(false);
	} else {
		// not found
		type_to_partition_index.insert({edge_type_id, oid});
	}
	edge_partitions.push_back(oid);
	string partition_dir_path = DiskAioParameters::WORKSPACE + "/part_" + std::to_string(pid);
	MkDir(partition_dir_path, true);
}

void GraphCatalogEntry::AddVertexPartition(ClientContext &context, PartitionID pid, idx_t oid, vector<VertexLabelID>& label_ids) {
	for (size_t i = 0; i < label_ids.size(); i++) {
		auto target_ids = label_to_partition_index.find(label_ids[i]);
		if (target_ids != label_to_partition_index.end()) {
			// found
			target_ids->second.push_back(oid);
		} else {
			// not found
			void_allocator void_alloc (context.db->GetCatalog().catalog_segment->get_segment_manager());
			idx_t_vector tmp_vec(void_alloc);
			tmp_vec.push_back(oid);
			label_to_partition_index.insert({label_ids[i], tmp_vec});
		}
	}
	vertex_partitions.push_back(oid);
	string partition_dir_path = DiskAioParameters::WORKSPACE + "/part_" + std::to_string(pid);
	MkDir(partition_dir_path, true);
}

void GraphCatalogEntry::AddVertexPartition(ClientContext &context, PartitionID pid, idx_t oid, vector<string> &labels) {
	char_allocator temp_charallocator (context.db->GetCatalog().catalog_segment->get_segment_manager());
	char_string label_(temp_charallocator);
	
	for (size_t i = 0; i < labels.size(); i++) {
		VertexLabelID vertex_label_id;
		label_ = labels[i].c_str();
		auto label_id = vertexlabel_map.find(label_);
		if (label_id != vertexlabel_map.end()) {
			// label found in label map
			vertex_label_id = label_id->second;
		} else {
			vertex_label_id = GetVertexLabelID();
			vertexlabel_map.insert({label_, vertex_label_id});
		}
		auto target_ids = label_to_partition_index.find(vertex_label_id);
		if (target_ids != label_to_partition_index.end()) {
			// found
			target_ids->second.push_back(oid);
		} else {
			// not found
			void_allocator void_alloc (context.db->GetCatalog().catalog_segment->get_segment_manager());
			idx_t_vector tmp_vec(void_alloc);
			tmp_vec.push_back(oid);
			label_to_partition_index.insert({vertex_label_id, tmp_vec});
		}
	}
	vertex_partitions.push_back(oid);
	string partition_dir_path = DiskAioParameters::WORKSPACE + "/part_" + std::to_string(pid);
	MkDir(partition_dir_path, true);
}

void GraphCatalogEntry::AddEdgeConnectionInfo(ClientContext &context, idx_t src_part_oid, idx_t edge_part_oid) {
	auto it = src_part_to_connected_edge_part_index.find(src_part_oid);
	if (it != src_part_to_connected_edge_part_index.end()) {
		// found
		it->second.push_back(edge_part_oid);
	} else {
		// not found
		void_allocator void_alloc (context.db->GetCatalog().catalog_segment->get_segment_manager());
		idx_t_vector tmp_vec(void_alloc);
		tmp_vec.push_back(edge_part_oid);
		src_part_to_connected_edge_part_index.insert({src_part_oid, tmp_vec});
	}
}

vector<idx_t> GraphCatalogEntry::Intersection(ClientContext &context, vector<VertexLabelID>& label_ids) {
	void_allocator void_alloc (context.db->GetCatalog().catalog_segment->get_segment_manager());
	vector<idx_t> curr_intersection;
	vector<idx_t> last_intersection;
	for (std::size_t i = 0; i < label_to_partition_index.at(label_ids[0]).size(); i++) {
		last_intersection.push_back(label_to_partition_index.at(label_ids[0])[i]);
	}

    for (std::size_t i = 1; i < label_ids.size(); ++i) {
        std::set_intersection(last_intersection.begin(), last_intersection.end(),
            label_to_partition_index.at(label_ids[i]).begin(), label_to_partition_index.at(label_ids[i]).end(),
            std::back_inserter(curr_intersection));
        std::swap(last_intersection, curr_intersection);
        curr_intersection.clear();
    }
    return last_intersection;
}

// TODO avoid copy & change name keys -> labelset_name?
vector<idx_t> GraphCatalogEntry::LookupPartition(ClientContext &context, vector<string> keys, GraphComponentType graph_component_type) {
	char_allocator temp_charallocator (context.db->GetCatalog().catalog_segment->get_segment_manager());
	char_string key_(temp_charallocator);
	vector<idx_t> return_pids;

	if (graph_component_type == GraphComponentType::EDGE) {
		D_ASSERT(keys.size() <= 1);
		if (keys.size() == 0) { // get all edges
			for (size_t i = 0; i < edge_partitions.size(); i++)
				return_pids.push_back(edge_partitions[i]);
			return return_pids;
		}
		key_ = keys[0].c_str();
		auto target_id = edgetype_map.find(key_);
		if (target_id != edgetype_map.end()) {
			return_pids.push_back(type_to_partition_index[target_id->second]);
		} else {
			throw InvalidInputException("There is no edge with the given type");
		}
	} else if (graph_component_type == GraphComponentType::VERTEX) {
		size_t key_len = keys.size();
		if (key_len == 0) { // get all vertices
			for (size_t i = 0; i < vertex_partitions.size(); i++)
				return_pids.push_back(vertex_partitions[i]);
			return return_pids;
		}
		vector<VertexLabelID> label_ids;
		bool not_found = false;
		for (size_t i = 0; i < key_len; i++) {
			key_ = keys[i].c_str();
			auto target_id = vertexlabel_map.find(key_);
			if (target_id != vertexlabel_map.end()) {
				label_ids.push_back(target_id->second);
			} else {
                std::string error_msg =
                    "There is no vertex with the given label " + keys[i] +
                    "\nPossible vertex labels: \n";
                for (auto &vlabel : vertexlabel_map) {
                    error_msg += "\t" + vlabel.first + "\n";
                }
                throw InvalidInputException(error_msg);
            }
		}
		return_pids = Intersection(context, label_ids);
	}
	return return_pids;
}

void GraphCatalogEntry::GetPropertyKeyIDs(ClientContext &context, vector<string> &property_names, vector<LogicalType> &property_types, vector<PropertyKeyID> &property_key_ids) {
	char_allocator temp_charallocator (context.db->GetCatalog().catalog_segment->get_segment_manager());
	char_string property_schema_(temp_charallocator);

	D_ASSERT(property_names.size() == property_types.size());

	if (property_key_id_to_name_vec.size() == 0) {
		// reserve 0 for '_id'
		PropertyKeyID new_pkid = 0;
		char_string property_sysid(temp_charallocator);
		property_sysid = std::string("_id").c_str();
		propertykey_map.insert(std::make_pair(property_sysid, new_pkid));
		propertykey_to_typeid_map.insert(std::make_pair(new_pkid, (uint8_t)LogicalTypeId::ID));
		property_key_id_to_name_vec.push_back(property_sysid);
	}
	
	for (int i = 0; i < property_names.size(); i++) {
		property_schema_ = property_names[i].c_str();
		auto property_key_id = propertykey_map.find(property_schema_);
		if (property_key_id != propertykey_map.end()) {
			property_key_ids.push_back(property_key_id->second);
		} else {
			PropertyKeyID new_pkid = GetPropertyKeyID(); // TODO key name + type ==> prop key id
			uint8_t type_id = (uint8_t) property_types[i].id();
			propertykey_map.insert(std::make_pair(property_schema_, new_pkid));
			propertykey_to_typeid_map.insert(std::make_pair(new_pkid, type_id));
			property_key_ids.push_back(new_pkid);
			property_key_id_to_name_vec.push_back(property_schema_);
		}
	}
}

void GraphCatalogEntry::GetPropertyNames(ClientContext &context, vector<PropertyKeyID> &property_key_ids,
	vector<string> &property_names)
{
	property_names.reserve(property_key_ids.size());
	for (int i = 0; i < property_key_ids.size(); i++) {
		auto property_name = property_key_id_to_name_vec[property_key_ids[i]];
		property_names.push_back(property_name);
	}
}

void GraphCatalogEntry::GetPropertyNames(ClientContext &context, PropertyKeyID_vector &property_key_ids, vector<string> &property_names) {
	property_names.reserve(property_key_ids.size());
	for (int i = 0; i < property_key_ids.size(); i++) {
		auto property_name = property_key_id_to_name_vec[property_key_ids[i]];
		property_names.push_back(property_name);
	}
}

string GraphCatalogEntry::GetPropertyName(ClientContext &context, PropertyKeyID property_key_id) {
	return property_key_id_to_name_vec[property_key_id];
}

void GraphCatalogEntry::GetVertexLabels(vector<string> &label_names) {
	for(auto it = vertexlabel_map.begin(); it != vertexlabel_map.end(); ++it) {
		label_names.push_back(it->first);
	}
}

void GraphCatalogEntry::GetEdgeTypes(vector<string> &type_names) {
	for(auto it = edgetype_map.begin(); it != edgetype_map.end(); ++it) {
		type_names.push_back(it->first);
	}
}

void GraphCatalogEntry::GetVertexPartitionIndexesInLabel(ClientContext &context, string label, vector<idx_t> &partition_indexes) {
	char_allocator temp_charallocator (context.db->GetCatalog().catalog_segment->get_segment_manager());
	char_string label_temp(temp_charallocator);
	label_temp = label.c_str();

	auto vertex_label_it = vertexlabel_map.find(label_temp);
	if (vertex_label_it != vertexlabel_map.end()) {
		idx_t_vector& cat_partition_indexes = label_to_partition_index.find(vertex_label_it->second)->second;
		partition_indexes.reserve(cat_partition_indexes.size());
		for (auto& partition_index : cat_partition_indexes) {
			partition_indexes.push_back(partition_index);
		}
	}
}

string GraphCatalogEntry::GetLabelFromVertexPartitionIndex(ClientContext &context, idx_t index) {
    char_allocator temp_charallocator(context.db->GetCatalog().catalog_segment->get_segment_manager());
    char_string label_found(temp_charallocator);

    bool found = false;
    for (auto& vertex_label_pair : vertexlabel_map) {
        auto& current_label = vertex_label_pair.first;
        auto& partition_indices = label_to_partition_index.find(vertex_label_pair.second)->second;

        if (std::find(partition_indices.begin(), partition_indices.end(), index) != partition_indices.end()) {
            label_found = current_label;
            found = true;
            break;
        }
    }

    if (found) {
        return label_found.c_str();
    } else {
       	return string();
    }
}

string GraphCatalogEntry::GetTypeFromEdgePartitionIndex(ClientContext &context, idx_t index) {
    char_allocator temp_charallocator(context.db->GetCatalog().catalog_segment->get_segment_manager());
    char_string type_found(temp_charallocator);

    bool found = false;
    for (auto& edge_type_pair : edgetype_map) {
        auto& current_type = edge_type_pair.first;
        auto& partition_index = type_to_partition_index.find(edge_type_pair.second)->second;

		if (partition_index == index) {
			type_found = current_type;
			found = true;
			break;
		}
    }

    if (found) {
        return type_found.c_str();
    } else {
       	return string();
    }
}

void GraphCatalogEntry::GetEdgePartitionIndexesInType(ClientContext &context, string type, vector<idx_t> &partition_indexes) {
	char_allocator temp_charallocator (context.db->GetCatalog().catalog_segment->get_segment_manager());
	char_string type_temp(temp_charallocator);
	type_temp = type.c_str();

	auto edge_type_it = edgetype_map.find(type_temp);
	if (edge_type_it != edgetype_map.end()) {
		idx_t &cat_partition_index = type_to_partition_index.find(edge_type_it->second)->second;
		partition_indexes.push_back(cat_partition_index);
	}
}

void GraphCatalogEntry::GetConnectedEdgeOids(ClientContext &context, idx_t src_part_oid, vector<idx_t> &edge_part_oids) {
	auto it = src_part_to_connected_edge_part_index.find(src_part_oid);
	if (it != src_part_to_connected_edge_part_index.end()) {
		auto &oids_vec = it->second;
		for (auto i = 0; i < oids_vec.size(); i++) {
			edge_part_oids.push_back(oids_vec[i]);
		}
	}
}

LogicalTypeId GraphCatalogEntry::GetTypeIdFromPropertyKeyID(const PropertyKeyID pkid) {
	auto it = propertykey_to_typeid_map.find(pkid);
	D_ASSERT(it != propertykey_to_typeid_map.end());
	D_ASSERT(it->second < std::numeric_limits<uint8_t>::max());
	uint8_t type_id = (uint8_t)it->second;
	return (LogicalTypeId)type_id;
}

VertexLabelID GraphCatalogEntry::GetVertexLabelID() {
	return vertex_label_id_version++;
}

EdgeTypeID GraphCatalogEntry::GetEdgeTypeID() {
	return edge_type_id_version++;
}

PropertyKeyID GraphCatalogEntry::GetPropertyKeyID() {
	return property_key_id_version++;
}

PartitionID GraphCatalogEntry::GetNewPartitionID() {
	return partition_id_version++;
}

PropertyKeyID GraphCatalogEntry::GetPropertyKeyID(ClientContext &context,
                                                  string &property_name)
{
    char_allocator temp_charallocator(
        context.db->GetCatalog().catalog_segment->get_segment_manager());
    char_string property_name_(temp_charallocator);
    property_name_ = property_name.c_str();

    // find property key id. do not allow to get property key id for a property that does not exist
    auto property_key_id = propertykey_map.find(property_name_);
    D_ASSERT(property_key_id != propertykey_map.end());

    return property_key_id->second;
}

PropertyKeyID GraphCatalogEntry::GetPropertyKeyID(ClientContext &context,
                                                  const string &property_name)
{
    char_allocator temp_charallocator(
        context.db->GetCatalog().catalog_segment->get_segment_manager());
    char_string property_name_(temp_charallocator);
    property_name_ = property_name.c_str();

    // find property key id. do not allow to get property key id for a property that does not exist
    auto property_key_id = propertykey_map.find(property_name_);
    D_ASSERT(property_key_id != propertykey_map.end());

    return property_key_id->second;
}

} // namespace duckdb
