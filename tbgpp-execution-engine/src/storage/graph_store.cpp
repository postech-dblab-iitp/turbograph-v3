#include <cassert>
#include <algorithm>
#include <vector>
#include <boost/algorithm/string.hpp>

#include "typedef.hpp"
#include "storage/graph_store.hpp"
#include "common/vector_size.hpp"
#include "common/boost_typedefs.hpp"
#include "main/database.hpp"
#include "main/client_context.hpp"
#include "catalog/catalog.hpp"
#include "catalog/catalog_entry/list.hpp"
#include "extent/extent_iterator.hpp"
#include "extent/adjlist_iterator.hpp"

#include "icecream.hpp"

namespace duckdb {

iTbgppGraphStore::iTbgppGraphStore(ClientContext &client) : client(client) {}

StoreAPIResult
iTbgppGraphStore::InitializeScan(std::queue<ExtentIterator *> &ext_its, vector<idx_t> &oids, vector<vector<uint64_t>> &projection_mapping,
	vector<vector<duckdb::LogicalType>> &scanSchemas) {
	Catalog &cat_instance = client.db->GetCatalog();
	D_ASSERT(oids.size() == projection_mapping.size());
	
	vector<vector<idx_t>> column_idxs;
	for (idx_t i = 0; i < oids.size(); i++) {
		PropertySchemaCatalogEntry *ps_cat_entry = // TODO get this in compilation process?
      		(PropertySchemaCatalogEntry *)cat_instance.GetEntry(client, DEFAULT_SCHEMA, oids[i]);
		
		auto ext_it = new ExtentIterator();
		ext_it->Initialize(client, ps_cat_entry, scanSchemas[i], projection_mapping[i]);
		ext_its.push(ext_it);
	}
	
	return StoreAPIResult::OK;
}

StoreAPIResult
iTbgppGraphStore::InitializeScan(std::queue<ExtentIterator *> &ext_its, PropertySchemaID_vector *oids, vector<vector<uint64_t>> &projection_mapping,
	vector<vector<duckdb::LogicalType>> &scanSchemas) {
	Catalog &cat_instance = client.db->GetCatalog();
	D_ASSERT(oids->size() == projection_mapping.size());
	
	vector<vector<idx_t>> column_idxs;
	for (idx_t i = 0; i < oids->size(); i++) {
		PropertySchemaCatalogEntry *ps_cat_entry = // TODO get this in compilation process?
      		(PropertySchemaCatalogEntry *)cat_instance.GetEntry(client, DEFAULT_SCHEMA, oids->at(i));
		
		auto ext_it = new ExtentIterator();
		ext_it->Initialize(client, ps_cat_entry, scanSchemas[i], projection_mapping[i]);
		ext_its.push(ext_it);
	}
	
	return StoreAPIResult::OK;
}

/**
 * Scan without filter pushdown
*/
StoreAPIResult
iTbgppGraphStore::doScan(std::queue<ExtentIterator *> &ext_its, duckdb::DataChunk &output, std::vector<duckdb::LogicalType> &scanSchema) {
	ExtentID current_eid;
	auto ext_it = ext_its.front();
	bool scan_ongoing = ext_it->GetNextExtent(client, output, current_eid);
	if (scan_ongoing) {
		return StoreAPIResult::OK;
	} else {
		ext_its.pop();
		delete ext_it;
		return StoreAPIResult::DONE;
	}
}

StoreAPIResult
iTbgppGraphStore::doScan(std::queue<ExtentIterator *> &ext_its, duckdb::DataChunk &output, vector<vector<uint64_t>> &projection_mapping,
	std::vector<duckdb::LogicalType> &scanSchema, int64_t current_schema_idx, bool is_output_initialized) {
	ExtentID current_eid;
	auto ext_it = ext_its.front();
	bool scan_ongoing = ext_it->GetNextExtent(client, output, current_eid, projection_mapping[current_schema_idx],
		EXEC_ENGINE_VECTOR_SIZE, is_output_initialized);
	if (scan_ongoing) {
		return StoreAPIResult::OK;
	} else {
		ext_its.pop();
		delete ext_it;
		return StoreAPIResult::DONE;
	}
}

/**
 * Scan with filter pushdown
*/
StoreAPIResult
iTbgppGraphStore::doScan(std::queue<ExtentIterator *> &ext_its, duckdb::DataChunk &output, vector<vector<uint64_t>> &projection_mapping, 
						std::vector<duckdb::LogicalType> &scanSchema, int64_t current_schema_idx, int64_t &filterKeyColIdx, duckdb::Value &filterValue) {
	ExtentID current_eid;
	auto ext_it = ext_its.front();
	bool scan_ongoing = ext_it->GetNextExtent(client, output, current_eid, filterKeyColIdx, filterValue, 
										projection_mapping[current_schema_idx], scanSchema, EXEC_ENGINE_VECTOR_SIZE); 
	if (scan_ongoing) {
		return StoreAPIResult::OK;
	} else {
		ext_its.pop();
		delete ext_it;
		return StoreAPIResult::DONE;
	}
}

StoreAPIResult
iTbgppGraphStore::doScan(std::queue<ExtentIterator *> &ext_its, duckdb::DataChunk &output, vector<vector<uint64_t>> &projection_mapping, 
					std::vector<duckdb::LogicalType> &scanSchema, int64_t current_schema_idx, int64_t &filterKeyColIdx, duckdb::RangeFilterValue &rangeFilterValue) {
	ExtentID current_eid;
	auto ext_it = ext_its.front();
	bool scan_ongoing = ext_it->GetNextExtent(client, output, current_eid, filterKeyColIdx, rangeFilterValue.l_value, rangeFilterValue.r_value, 
											rangeFilterValue.l_inclusive, rangeFilterValue.r_inclusive, projection_mapping[current_schema_idx], 
											scanSchema, EXEC_ENGINE_VECTOR_SIZE); 
	if (scan_ongoing) {
		return StoreAPIResult::OK;
	} else {
		/* Move to next ExtentIterator means the scan for the schema has finished */
		ext_its.pop();
		delete ext_it;
		return StoreAPIResult::DONE;
	}
}

StoreAPIResult 
iTbgppGraphStore::InitializeVertexIndexSeek(std::queue<ExtentIterator *> &ext_its, vector<idx_t> &oids, vector<vector<uint64_t>> &projection_mapping, DataChunk &input, idx_t nodeColIdx, std::vector<duckdb::LogicalType> &scanSchema, vector<ExtentID> &target_eids, vector<idx_t> &boundary_position) {
	Catalog &cat_instance = client.db->GetCatalog();
	
	vector<vector<idx_t>> column_idxs;
	for (idx_t i = 0; i < oids.size(); i++) {
		ExtentID prev_eid = input.size() == 0 ? 0 : (UBigIntValue::Get(input.GetValue(nodeColIdx, 0)) >> 32);
		for (size_t i = 0; i < input.size(); i++) {
			uint64_t vid = UBigIntValue::Get(input.GetValue(nodeColIdx, i));
			ExtentID target_eid = GET_EID_FROM_PHYSICAL_ID(vid);
			target_eids.push_back(target_eid);
			if (prev_eid != target_eid) boundary_position.push_back(i - 1);
			prev_eid = target_eid;
		}
		boundary_position.push_back(input.size() - 1);
		target_eids.erase( std::unique( target_eids.begin(), target_eids.end() ), target_eids.end() );

		if (target_eids.size() == 0) return StoreAPIResult::DONE;

		auto ext_it = new ExtentIterator();
		// ext_it->Initialize(client, scanSchema, projection_mapping[i], target_eids);
		ext_its.push(ext_it);
	}

	return StoreAPIResult::OK;
}

void
iTbgppGraphStore::_fillTargetSeqnosVecAndBoundaryPosition(idx_t i, ExtentID prev_eid, unordered_map<ExtentID, vector<idx_t>> &target_seqnos_per_extent_map, vector<idx_t> &boundary_position) {
	idx_t begin_idx = boundary_position.size() == 0 ? 0 : boundary_position.back() + 1;
	idx_t end_idx = i - 1;
	auto it = target_seqnos_per_extent_map.find(prev_eid);
	if (it == target_seqnos_per_extent_map.end()) {
		vector<idx_t> tmp_vec;
		for (idx_t j = begin_idx; j <= end_idx; j++)
			tmp_vec.push_back(j);
		target_seqnos_per_extent_map.insert({prev_eid, std::move(tmp_vec)});
	} else {
		auto &vec = it->second;
		for (idx_t j = begin_idx; j <= end_idx; j++)
			vec.push_back(j);
	}
	boundary_position.push_back(i - 1);
}

StoreAPIResult 
iTbgppGraphStore::InitializeVertexIndexSeek(std::queue<ExtentIterator *> &ext_its, vector<idx_t> &oids, vector<vector<uint64_t>> &projection_mapping,
											DataChunk &input, idx_t nodeColIdx, vector<vector<LogicalType>> &scanSchemas, vector<ExtentID> &target_eids,
											vector<vector<idx_t>> &target_seqnos_per_extent, unordered_map<idx_t, idx_t> &ps_oid_to_projection_mapping,
											vector<idx_t> &mapping_idxs) {
	Catalog &cat_instance = client.db->GetCatalog();
	vector<idx_t> boundary_position;
	unordered_map<ExtentID, vector<idx_t>> target_seqnos_per_extent_map;
	ExtentID prev_eid = input.size() == 0 ? 0 : (UBigIntValue::Get(input.GetValue(nodeColIdx, 0)) >> 32);
	Vector& src_vid_column_vector = input.data[nodeColIdx];
	target_eids.push_back(prev_eid);
	switch (src_vid_column_vector.GetVectorType()) {
		case VectorType::DICTIONARY_VECTOR: {
			for (size_t i = 0; i < input.size(); i++) {
				uint64_t vid = ((uint64_t *)src_vid_column_vector.GetData())[DictionaryVector::SelVector(src_vid_column_vector).get_index(i)];
				ExtentID target_eid = GET_EID_FROM_PHYSICAL_ID(vid);
				if (prev_eid != target_eid) {
					target_eids.push_back(target_eid);
					_fillTargetSeqnosVecAndBoundaryPosition(i, prev_eid, target_seqnos_per_extent_map, boundary_position);
				}
				prev_eid = target_eid;
			}
			break;
		}
		case VectorType::FLAT_VECTOR: {
			for (size_t i = 0; i < input.size(); i++) {
				uint64_t vid = ((uint64_t *)src_vid_column_vector.GetData())[i];
				ExtentID target_eid = GET_EID_FROM_PHYSICAL_ID(vid);
				if (prev_eid != target_eid) {
					target_eids.push_back(target_eid);
					_fillTargetSeqnosVecAndBoundaryPosition(i, prev_eid, target_seqnos_per_extent_map, boundary_position);
				}
				prev_eid = target_eid;
			}
			break;
		}
		case VectorType::CONSTANT_VECTOR: {
			for (size_t i = 0; i < input.size(); i++) {
				uint64_t vid = ((uint64_t *)ConstantVector::GetData<uintptr_t>(src_vid_column_vector))[0];
				ExtentID target_eid = GET_EID_FROM_PHYSICAL_ID(vid);
				if (prev_eid != target_eid) {
					target_eids.push_back(target_eid);
					_fillTargetSeqnosVecAndBoundaryPosition(i, prev_eid, target_seqnos_per_extent_map, boundary_position);
				}
				prev_eid = target_eid;
			}
			break;
		}
		default: {
			D_ASSERT(false);
		}
	}

	// process remaining
	_fillTargetSeqnosVecAndBoundaryPosition(input.size(), prev_eid, target_seqnos_per_extent_map, boundary_position);

	// remove redundant eids
	std::sort(target_eids.begin(), target_eids.end());
	target_eids.erase(std::unique(target_eids.begin(), target_eids.end()), target_eids.end());
	if (target_eids.size() == 0) return StoreAPIResult::DONE;

	for (auto i = 0; i < target_eids.size(); i++) {
		string ext_name = DEFAULT_EXTENT_PREFIX + std::to_string(target_eids[i]);
		ExtentCatalogEntry *ext_cat =
			(ExtentCatalogEntry *)cat_instance.GetEntry(client, CatalogType::EXTENT_ENTRY, DEFAULT_SCHEMA, ext_name);
		idx_t psid = ext_cat->ps_oid;
		auto mapping_idx = ps_oid_to_projection_mapping.at(psid);
		
		mapping_idxs.push_back(mapping_idx);
		target_seqnos_per_extent.push_back(std::move(target_seqnos_per_extent_map.at(target_eids[i])));
	}

	auto ext_it = new ExtentIterator();
	ext_it->Initialize(client, scanSchemas, projection_mapping, mapping_idxs, target_eids);
	ext_its.push(ext_it);

	return StoreAPIResult::OK;
}

StoreAPIResult
iTbgppGraphStore::doVertexIndexSeek(ExtentIterator *&ext_it, DataChunk& output, DataChunk &input, idx_t nodeColIdx, LabelSet labels, std::vector<LabelSet> &edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> &scanSchema, vector<ExtentID> &target_eids, vector<idx_t> &boundary_position, idx_t current_pos, vector<idx_t> output_col_idx) {
	D_ASSERT(ext_it != nullptr || ext_it->IsInitialized());
	ExtentID target_eid = target_eids[current_pos]; // TODO make this functionality as Macro --> GetEIDFromPhysicalID
	ExtentID current_eid;
	if (current_pos >= boundary_position.size()) throw InvalidInputException("??");
	idx_t start_seqno, end_seqno; // [start_seqno, end_seqno]
	start_seqno = current_pos == 0 ? 0 : boundary_position[current_pos - 1] + 1;
	end_seqno = boundary_position[current_pos];
	bool scan_ongoing = ext_it->GetNextExtent(client, output, current_eid, target_eid, input, nodeColIdx, output_col_idx, start_seqno, end_seqno);

	// // IC(vid, target_eid, target_seqno, current_eid);
	if (scan_ongoing) assert(current_eid == target_eid);
	
	return StoreAPIResult::OK;
}

StoreAPIResult
iTbgppGraphStore::doVertexIndexSeek(std::queue<ExtentIterator *> &ext_its, DataChunk& output, DataChunk &input,
									idx_t nodeColIdx, std::vector<duckdb::LogicalType> &scanSchema, vector<ExtentID> &target_eids,
									vector<idx_t> &boundary_position, idx_t current_pos, vector<idx_t> output_col_idx) {
	ExtentID target_eid = target_eids[current_pos]; // TODO make this functionality as Macro --> GetEIDFromPhysicalID
	ExtentID current_eid;
	auto ext_it = ext_its.front();
	D_ASSERT(ext_it != nullptr || ext_it->IsInitialized());
	if (current_pos >= boundary_position.size()) throw InvalidInputException("??");
	idx_t start_seqno, end_seqno; // [start_seqno, end_seqno]
	start_seqno = current_pos == 0 ? 0 : boundary_position[current_pos - 1] + 1;
	end_seqno = boundary_position[current_pos];
	bool scan_ongoing = ext_it->GetNextExtent(client, output, current_eid, target_eid, input, nodeColIdx, output_col_idx, start_seqno, end_seqno);

	if (scan_ongoing) {
		//output.Reference(*output_);
		D_ASSERT(current_eid == target_eid);
		return StoreAPIResult::OK;
	} else {
		ext_its.pop();
		delete ext_it;
		if (ext_its.size() > 0)
			return StoreAPIResult::OK;
		else
			return StoreAPIResult::DONE;
	}
}

StoreAPIResult
iTbgppGraphStore::doVertexIndexSeek(std::queue<ExtentIterator *> &ext_its, DataChunk& output, DataChunk &input,
									idx_t nodeColIdx, std::vector<duckdb::LogicalType> &scanSchema, vector<ExtentID> &target_eids,
									vector<vector<idx_t>> &target_seqnos_per_extent, idx_t current_pos, vector<idx_t> output_col_idx) {
	ExtentID target_eid = target_eids[current_pos]; // TODO make this functionality as Macro --> GetEIDFromPhysicalID
	ExtentID current_eid;
	auto ext_it = ext_its.front();
	D_ASSERT(ext_it != nullptr || ext_it->IsInitialized());
	D_ASSERT(current_pos < target_seqnos_per_extent.size());
	bool scan_ongoing = ext_it->GetNextExtent(client, output, current_eid, target_eid, input, nodeColIdx, 
		output_col_idx, target_seqnos_per_extent[current_pos]);

	if (scan_ongoing) {
		//output.Reference(*output_);
		D_ASSERT(current_eid == target_eid);
		return StoreAPIResult::OK;
	} else {
		ext_its.pop();
		delete ext_it;
		if (ext_its.size() > 0)
			return StoreAPIResult::OK;
		else
			return StoreAPIResult::DONE;
	}
}

StoreAPIResult
iTbgppGraphStore::doVertexIndexSeek(std::queue<ExtentIterator *> &ext_its, DataChunk& output, DataChunk &input,
									idx_t nodeColIdx, std::vector<duckdb::LogicalType> &scanSchema, vector<ExtentID> &target_eids,
									vector<vector<idx_t>> &target_seqnos_per_extent, idx_t current_pos, vector<idx_t> output_col_idx,
									idx_t &output_idx, SelectionVector &sel, int64_t &filterKeyColIdx, duckdb::Value &filterValue) {
	ExtentID target_eid = target_eids[current_pos]; // TODO make this functionality as Macro --> GetEIDFromPhysicalID
	ExtentID current_eid;
	auto ext_it = ext_its.front();
	D_ASSERT(ext_it != nullptr || ext_it->IsInitialized());
	D_ASSERT(current_pos < target_seqnos_per_extent.size());
	bool scan_ongoing = ext_it->GetNextExtent(client, output, current_eid, filterKeyColIdx, filterValue, target_eid, input,
											  nodeColIdx, output_col_idx, target_seqnos_per_extent[current_pos], output_idx, sel);

	if (scan_ongoing) {
		D_ASSERT(current_eid == target_eid);
		return StoreAPIResult::OK;
	} else {
		ext_its.pop();
		delete ext_it;
		if (ext_its.size() > 0)
			return StoreAPIResult::OK;
		else
			return StoreAPIResult::DONE;
	}
}

StoreAPIResult
iTbgppGraphStore::InitializeEdgeIndexSeek(ExtentIterator *&ext_it, DataChunk& output, uint64_t vid, LabelSet labels, std::vector<LabelSet> &edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> &scanSchema) {
	D_ASSERT(ext_it == nullptr);
	Catalog &cat_instance = client.db->GetCatalog();
	D_ASSERT(labels.size() == 1); // XXX Temporary
	string entry_name = "eps_";
	for (auto &it : labels.data) entry_name += it;
	PropertySchemaCatalogEntry* ps_cat_entry = 
      (PropertySchemaCatalogEntry*) cat_instance.GetEntry(client, CatalogType::PROPERTY_SCHEMA_ENTRY, DEFAULT_SCHEMA, entry_name);

	D_ASSERT(edgeLabels.size() <= 1); // XXX Temporary
	vector<string> properties_temp;
	for (size_t i = 0; i < edgeLabels.size(); i++) {
		for (auto &it : edgeLabels[i].data) properties_temp.push_back(it);
	}
	for (auto &it : properties) {
		// std::cout << "Property: " << it << std::endl;
		properties_temp.push_back(it);
	}
	vector<idx_t> column_idxs;
	column_idxs = move(ps_cat_entry->GetColumnIdxs(properties_temp));

	ExtentID target_eid = GET_EID_FROM_PHYSICAL_ID(vid);
	idx_t target_seqno = GET_SEQNO_FROM_PHYSICAL_ID(vid);

	ext_it = new ExtentIterator();
	ext_it->Initialize(client, scanSchema, column_idxs, target_eid);
	return StoreAPIResult::OK;
}

StoreAPIResult
iTbgppGraphStore::InitializeEdgeIndexSeek(ExtentIterator *&ext_it, DataChunk& output, DataChunk &input, idx_t nodeColIdx, LabelSet labels, std::vector<LabelSet> &edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> &scanSchema, vector<ExtentID> &target_eids, vector<idx_t> &boundary_position) {
	D_ASSERT(false); // temporary
	D_ASSERT(ext_it == nullptr);
	Catalog &cat_instance = client.db->GetCatalog();
	D_ASSERT(labels.size() == 1); // XXX Temporary
	string entry_name = "eps_";
	for (auto &it : labels.data) entry_name += it;
	PropertySchemaCatalogEntry* ps_cat_entry = 
      (PropertySchemaCatalogEntry*) cat_instance.GetEntry(client, CatalogType::PROPERTY_SCHEMA_ENTRY, DEFAULT_SCHEMA, entry_name);

	D_ASSERT(edgeLabels.size() <= 1); // XXX Temporary
	vector<string> properties_temp;
	for (size_t i = 0; i < edgeLabels.size(); i++) {
		for (auto &it : edgeLabels[i].data) properties_temp.push_back(it);
	}
	for (auto &it : properties) {
		// std::cout << "Property: " << it << std::endl;
		properties_temp.push_back(it);
	}
	vector<idx_t> column_idxs;
	column_idxs = move(ps_cat_entry->GetColumnIdxs(properties_temp));

	ExtentID prev_eid = input.size() == 0 ? 0 : (UBigIntValue::Get(input.GetValue(nodeColIdx, 0)) >> 32);
	for (size_t i = 0; i < input.size(); i++) {
		uint64_t vid = UBigIntValue::Get(input.GetValue(nodeColIdx, i));
		ExtentID target_eid = vid >> 32; // TODO make this functionality as Macro --> GetEIDFromPhysicalID
		target_eids.push_back(target_eid);
		if (prev_eid != target_eid) boundary_position.push_back(i - 1);
		prev_eid = target_eid;
	}
	boundary_position.push_back(input.size() - 1);

	target_eids.erase( std::unique( target_eids.begin(), target_eids.end() ), target_eids.end() );

	if (target_eids.size() == 0) return StoreAPIResult::DONE;

	ext_it = new ExtentIterator();
	// ext_it->Initialize(client, scanSchema, column_idxs, target_eids);
	return StoreAPIResult::OK;
}

StoreAPIResult
iTbgppGraphStore::doEdgeIndexSeek(ExtentIterator *&ext_it, DataChunk& output, DataChunk &input, idx_t nodeColIdx, LabelSet labels, std::vector<LabelSet> &edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> &scanSchema, vector<ExtentID> &target_eids, vector<idx_t> &boundary_position, idx_t current_pos, vector<idx_t> output_col_idx) {
	D_ASSERT(ext_it != nullptr || ext_it->IsInitialized());
	ExtentID target_eid = target_eids[current_pos]; // TODO make this functionality as Macro --> GetEIDFromPhysicalID
	ExtentID current_eid;
	if (current_pos >= boundary_position.size()) throw InvalidInputException("??");
	idx_t start_seqno, end_seqno; // [start_seqno, end_seqno]
	start_seqno = current_pos == 0 ? 0 : boundary_position[current_pos - 1] + 1;
	end_seqno = boundary_position[current_pos];
	bool scan_ongoing = ext_it->GetNextExtent(client, output, current_eid, target_eid, input, nodeColIdx, output_col_idx, start_seqno, end_seqno);

	// // IC(vid, target_eid, target_seqno, current_eid);
	if (scan_ongoing) assert(current_eid == target_eid);
	
	return StoreAPIResult::OK;
}

bool
iTbgppGraphStore::isNodeInLabelset(u_int64_t id, LabelSet labels) {
	return true;
}

void
iTbgppGraphStore::getAdjColIdxs(idx_t index_cat_oid, vector<int> &adjColIdxs, vector<LogicalType> &adjColTypes) {
	Catalog &cat_instance = client.db->GetCatalog();
	IndexCatalogEntry *index_cat = 
		(IndexCatalogEntry *)cat_instance.GetEntry(client, DEFAULT_SCHEMA, index_cat_oid);
	
	adjColIdxs.push_back(index_cat->GetAdjColIdx());

	if (index_cat->GetIndexType() == IndexType::FORWARD_CSR) {
		adjColTypes.push_back(LogicalType::FORWARD_ADJLIST);
	} else if (index_cat->GetIndexType() == IndexType::BACKWARD_CSR) {
		adjColTypes.push_back(LogicalType::BACKWARD_ADJLIST);
	} else {
		throw InvalidInputException("IndexType should be one of FORWARD/BACKWARD CSR");
	}
}

StoreAPIResult
iTbgppGraphStore::getAdjListFromVid(AdjacencyListIterator &adj_iter, int adjColIdx, ExtentID &prev_eid, uint64_t vid, uint64_t *&start_ptr, uint64_t *&end_ptr, ExpandDirection expand_dir) {
	D_ASSERT( expand_dir ==ExpandDirection::OUTGOING || expand_dir == ExpandDirection::INCOMING );
	bool is_initialized = true;
	ExtentID target_eid = vid >> 32;
	if (target_eid != prev_eid) {
		// initialize
		if (expand_dir == ExpandDirection::OUTGOING) {
			is_initialized = adj_iter.Initialize(client, adjColIdx, target_eid, LogicalType::FORWARD_ADJLIST);
		} else if (expand_dir == ExpandDirection::INCOMING) {
			is_initialized = adj_iter.Initialize(client, adjColIdx, target_eid, LogicalType::BACKWARD_ADJLIST);
		}
		adj_iter.getAdjListPtr(vid, target_eid, start_ptr, end_ptr, is_initialized);
	} else {
		adj_iter.getAdjListPtr(vid, target_eid, start_ptr, end_ptr, is_initialized);
	}
	prev_eid = target_eid;
	
	return StoreAPIResult::OK;
}

}