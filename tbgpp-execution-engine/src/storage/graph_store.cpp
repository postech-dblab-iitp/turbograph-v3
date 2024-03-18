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
#include "range/v3/all.hpp"

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
	D_ASSERT(false); // deprecated
}

void
iTbgppGraphStore::_fillTargetSeqnosVecAndBoundaryPosition(idx_t i, ExtentID prev_eid, unordered_map<ExtentID, vector<idx_t>> &target_seqnos_per_extent_map, vector<idx_t> &boundary_position, vector<idx_t> &tmp_vec) {
	auto it = target_seqnos_per_extent_map.find(prev_eid);
	if (it == target_seqnos_per_extent_map.end()) {
		target_seqnos_per_extent_map.insert({prev_eid, std::move(tmp_vec)});
	} else {
		auto &vec = it->second;
		for (auto &j : tmp_vec) vec.push_back(j);
	}
	boundary_position.push_back(i - 1);
	tmp_vec.clear();
}

StoreAPIResult iTbgppGraphStore::InitializeVertexIndexSeek(
    std::queue<ExtentIterator *> &ext_its, vector<idx_t> &oids,
    vector<vector<uint64_t>> &projection_mapping, DataChunk &input,
    idx_t nodeColIdx, vector<vector<LogicalType>> &scanSchemas,
    vector<ExtentID> &target_eids,
    vector<vector<idx_t>> &target_seqnos_per_extent,
    unordered_map<idx_t, idx_t> &ps_oid_to_projection_mapping,
    vector<idx_t> &mapping_idxs,
    std::unordered_map<ExtentID, idx_t> &eid_to_mapping_idx,
    vector<idx_t> &null_tuples_idx)
{
    Catalog &cat_instance = client.db->GetCatalog();
	vector<idx_t> boundary_position;
	vector<idx_t> tmp_vec;
	unordered_map<ExtentID, vector<idx_t>> target_seqnos_per_extent_map;
	ExtentID prev_eid = std::numeric_limits<ExtentID>::max();
	Vector &src_vid_column_vector = input.data[nodeColIdx];
	auto &validity = src_vid_column_vector.GetValidity();
	// target_eids.push_back(prev_eid);
	if (validity.AllValid()) {
		switch (src_vid_column_vector.GetVectorType()) {
			case VectorType::DICTIONARY_VECTOR: {
				for (size_t i = 0; i < input.size(); i++) {
					uint64_t vid = ((uint64_t *)src_vid_column_vector.GetData())[DictionaryVector::SelVector(src_vid_column_vector).get_index(i)];
					ExtentID target_eid = GET_EID_FROM_PHYSICAL_ID(vid);
					if (i == 0) prev_eid = target_eid;
					if (prev_eid != target_eid) {
						target_eids.push_back(prev_eid);
						_fillTargetSeqnosVecAndBoundaryPosition(i, prev_eid, target_seqnos_per_extent_map, boundary_position, tmp_vec);
					}
					tmp_vec.push_back(i);
					prev_eid = target_eid;
				}
				break;
			}
			case VectorType::FLAT_VECTOR: {
				for (size_t i = 0; i < input.size(); i++) {
					uint64_t vid = ((uint64_t *)src_vid_column_vector.GetData())[i];
					ExtentID target_eid = GET_EID_FROM_PHYSICAL_ID(vid);
					if (i == 0) prev_eid = target_eid;
					if (prev_eid != target_eid) {
						target_eids.push_back(prev_eid);
						_fillTargetSeqnosVecAndBoundaryPosition(i, prev_eid, target_seqnos_per_extent_map, boundary_position, tmp_vec);
					}
					tmp_vec.push_back(i);
					prev_eid = target_eid;
				}
				break;
			}
			case VectorType::CONSTANT_VECTOR: {
				for (size_t i = 0; i < input.size(); i++) {
					uint64_t vid = ((uint64_t *)ConstantVector::GetData<uintptr_t>(src_vid_column_vector))[0];
					ExtentID target_eid = GET_EID_FROM_PHYSICAL_ID(vid);
					if (i == 0) prev_eid = target_eid;
					if (prev_eid != target_eid) {
						target_eids.push_back(prev_eid);
						_fillTargetSeqnosVecAndBoundaryPosition(i, prev_eid, target_seqnos_per_extent_map, boundary_position, tmp_vec);
					}
					tmp_vec.push_back(i);
					prev_eid = target_eid;
				}
				break;
			}
			default: {
				D_ASSERT(false);
			}
		}
	} else if (validity.CheckAllInValid()) {
		D_ASSERT(false); // not implemented yet
		return StoreAPIResult::OK;
	} else {
		switch (src_vid_column_vector.GetVectorType()) {
			case VectorType::DICTIONARY_VECTOR: {
				for (size_t i = 0; i < input.size(); i++) {
					auto vid_val = src_vid_column_vector.GetValue(i);
					if (vid_val.IsNull()) {
						null_tuples_idx.push_back(i);
						continue;
					}
					uint64_t vid = vid_val.GetValue<uint64_t>();
					ExtentID target_eid = GET_EID_FROM_PHYSICAL_ID(vid);
					if (prev_eid == std::numeric_limits<ExtentID>::max()) prev_eid = target_eid;
					if (prev_eid != target_eid) {
						target_eids.push_back(prev_eid);
						_fillTargetSeqnosVecAndBoundaryPosition(i, prev_eid, target_seqnos_per_extent_map, boundary_position, tmp_vec);
					}
					tmp_vec.push_back(i);
					prev_eid = target_eid;
				}
			}
			case VectorType::FLAT_VECTOR: {
				for (size_t i = 0; i < input.size(); i++) {
					if (!validity.RowIsValid(i)) {
						null_tuples_idx.push_back(i);
						continue;
					}
					uint64_t vid = ((uint64_t *)src_vid_column_vector.GetData())[i];
					ExtentID target_eid = GET_EID_FROM_PHYSICAL_ID(vid);
					if (prev_eid == std::numeric_limits<ExtentID>::max()) prev_eid = target_eid;
					if (prev_eid != target_eid) {
						target_eids.push_back(prev_eid);
						_fillTargetSeqnosVecAndBoundaryPosition(i, prev_eid, target_seqnos_per_extent_map, boundary_position, tmp_vec);
					}
					tmp_vec.push_back(i);
					prev_eid = target_eid;
				}
				break;
			}
			case VectorType::CONSTANT_VECTOR: {
				D_ASSERT(false);
			}
			default: {
				D_ASSERT(false);
			}
		}
	}

	// process remaining
	if (tmp_vec.size() > 0) {
		target_eids.push_back(prev_eid);
		_fillTargetSeqnosVecAndBoundaryPosition(input.size(), prev_eid, target_seqnos_per_extent_map, boundary_position, tmp_vec);
	}

	// remove redundant eids
	std::sort(target_eids.begin(), target_eids.end());
	target_eids.erase(std::unique(target_eids.begin(), target_eids.end()), target_eids.end());
	if (target_eids.size() == 0) return StoreAPIResult::DONE;
	
	/**
	 * TODO: this code should be rmoved! this is temporal code
	 * Also, this is very slow due to GetEntry access.
	 * Optimize this.
	*/
	// remove extent ids to be removed due to filter
	vector<ExtentID> target_eids_after_remove;
	for (auto i = 0; i < target_eids.size(); i++) {
		string ext_name = DEFAULT_EXTENT_PREFIX + std::to_string(target_eids[i]);
		ExtentCatalogEntry *ext_cat =
			(ExtentCatalogEntry *)cat_instance.GetEntry(client, CatalogType::EXTENT_ENTRY, DEFAULT_SCHEMA, ext_name);
		idx_t psid = ext_cat->ps_oid;
		auto it = ps_oid_to_projection_mapping.find(psid);
		if (it != ps_oid_to_projection_mapping.end()) {
			target_eids_after_remove.push_back(target_eids[i]);
		}
	}
	target_eids = std::move(target_eids_after_remove);

	for (auto i = 0; i < target_eids.size(); i++) {
		idx_t mapping_idx = -1;
		auto eid_it = eid_to_mapping_idx.find(target_eids[i]);
		if (eid_it == eid_to_mapping_idx.end()) {
			string ext_name = DEFAULT_EXTENT_PREFIX + std::to_string(target_eids[i]);
			ExtentCatalogEntry *ext_cat =
				(ExtentCatalogEntry *)cat_instance.GetEntry(client, CatalogType::EXTENT_ENTRY, DEFAULT_SCHEMA, ext_name);
			idx_t psid = ext_cat->ps_oid;
			auto it = ps_oid_to_projection_mapping.find(psid);
			if (it == ps_oid_to_projection_mapping.end()) {
				throw InvalidInputException("Projection mapping not found for ps_oid: " + std::to_string(psid));
			}
			mapping_idx = it->second;
			eid_to_mapping_idx.insert({target_eids[i], mapping_idx});
		}
		else {
			mapping_idx = eid_it->second;
		}
		D_ASSERT(mapping_idx != -1);
		mapping_idxs.push_back(mapping_idx);
		target_seqnos_per_extent.push_back(std::move(target_seqnos_per_extent_map.at(target_eids[i])));
	}

	// TODO maybe we don't need this..
	ranges::sort(
		ranges::views::zip(mapping_idxs, target_eids, target_seqnos_per_extent), 
		std::less{}, 
		[](auto && p){ return std::get<0>(p); }
	);

	auto ext_it = new ExtentIterator();
	ext_it->Initialize(client, scanSchemas, projection_mapping, mapping_idxs, target_eids);
	ext_its.push(ext_it);

	return StoreAPIResult::OK;
}

StoreAPIResult
iTbgppGraphStore::doVertexIndexSeek(std::queue<ExtentIterator *> &ext_its, DataChunk& output, DataChunk &input,
									idx_t nodeColIdx, std::vector<duckdb::LogicalType> &scanSchema, vector<ExtentID> &target_eids,
									vector<vector<idx_t>> &target_seqnos_per_extent, idx_t current_pos, vector<idx_t> output_col_idx)
{
	if (ext_its.empty()) return StoreAPIResult::DONE;
	ExtentID target_eid = target_eids[current_pos];
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
									vector<vector<idx_t>> &target_seqnos_per_extent, idx_t current_pos, Vector &rowcol_vec,
									char *row_major_store)
{
	ExtentID target_eid = target_eids[current_pos];
	ExtentID current_eid;
	auto ext_it = ext_its.front();
	D_ASSERT(ext_it != nullptr || ext_it->IsInitialized());
	D_ASSERT(current_pos < target_seqnos_per_extent.size());
	bool scan_ongoing = ext_it->GetNextExtentInRowFormat(client, output, current_eid, target_eid, input, nodeColIdx, 
		rowcol_vec, row_major_store, target_seqnos_per_extent[current_pos]);

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
									idx_t &num_tuples_per_chunk)
{
	ExtentID target_eid = target_eids[current_pos];
	ExtentID current_eid;
	auto ext_it = ext_its.front();
	D_ASSERT(ext_it != nullptr || ext_it->IsInitialized());
	D_ASSERT(current_pos < target_seqnos_per_extent.size());
	bool scan_ongoing = ext_it->GetNextExtent(client, output, current_eid, target_eid, input, nodeColIdx, 
		output_col_idx, target_seqnos_per_extent[current_pos], num_tuples_per_chunk);

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
	ExtentID target_eid = target_eids[current_pos];
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