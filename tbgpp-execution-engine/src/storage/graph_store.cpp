#include <cassert>
#include <algorithm>
#include <vector>
#include <set>
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

iTbgppGraphStore::iTbgppGraphStore(ClientContext &client) : client(client), boundary_position(STANDARD_VECTOR_SIZE), 
	tmp_vec(STANDARD_VECTOR_SIZE), boundary_position_cursor(0),
	target_eid_flags(INITIAL_EXTENT_ID_SPACE), tmp_vec_cursor(0), 
	target_seqnos_per_extent_map(INITIAL_EXTENT_ID_SPACE, vector<uint32_t>(INITIAL_EXTENT_ID_SPACE)), target_seqnos_per_extent_map_cursors(INITIAL_EXTENT_ID_SPACE, 0) {}

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
iTbgppGraphStore::doScan(std::queue<ExtentIterator *> &ext_its, duckdb::DataChunk &output, FilteredChunkBuffer &output_buffer, vector<vector<uint64_t>> &projection_mapping, 
						std::vector<duckdb::LogicalType> &scanSchema, int64_t current_schema_idx, int64_t &filterKeyColIdx, duckdb::Value &filterValue) {
	ExtentID current_eid;
	auto ext_it = ext_its.front();
	bool scan_ongoing = ext_it->GetNextExtent(client, output, output_buffer, current_eid, filterKeyColIdx, filterValue, 
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
iTbgppGraphStore::doScan(std::queue<ExtentIterator *> &ext_its, duckdb::DataChunk &output, FilteredChunkBuffer &output_buffer, vector<vector<uint64_t>> &projection_mapping, 
					std::vector<duckdb::LogicalType> &scanSchema, int64_t current_schema_idx, int64_t &filterKeyColIdx, duckdb::RangeFilterValue &rangeFilterValue) {
	ExtentID current_eid;
	auto ext_it = ext_its.front();
	bool scan_ongoing = ext_it->GetNextExtent(client, output, output_buffer, current_eid, filterKeyColIdx, rangeFilterValue.l_value, rangeFilterValue.r_value, 
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
iTbgppGraphStore::doScan(std::queue<ExtentIterator *> &ext_its, duckdb::DataChunk &output, FilteredChunkBuffer &output_buffer, vector<vector<uint64_t>> &projection_mapping, 
					std::vector<duckdb::LogicalType> &scanSchema, int64_t current_schema_idx, ExpressionExecutor& executor) {
	ExtentID current_eid;
	auto ext_it = ext_its.front();
	bool scan_ongoing = ext_it->GetNextExtent(client, output, output_buffer, current_eid, executor, projection_mapping[current_schema_idx], 
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

inline void
iTbgppGraphStore::_fillTargetSeqnosVecAndBoundaryPosition(idx_t i, ExtentID prev_eid) {
	auto prev_eid_seqno = GET_EXTENT_SEQNO_FROM_EID(prev_eid);
	if (prev_eid_seqno > target_seqnos_per_extent_map.size()) {
		target_seqnos_per_extent_map.resize(prev_eid_seqno + 1); 
		target_seqnos_per_extent_map_cursors.resize(prev_eid_seqno + 1, 0);
	}
	vector<uint32_t> &vec = target_seqnos_per_extent_map[prev_eid_seqno];
	idx_t &cursor = target_seqnos_per_extent_map_cursors[prev_eid_seqno];
	for (auto i = 0; i < tmp_vec_cursor; i++) {
		vec[cursor++] = tmp_vec[i];
	}
	boundary_position[boundary_position_cursor++] = i - 1;
	tmp_vec_cursor = 0;
}

StoreAPIResult iTbgppGraphStore::InitializeVertexIndexSeek(
    ExtentIterator *&ext_it, vector<vector<uint64_t>> &projection_mapping, DataChunk &input,
    idx_t nodeColIdx, vector<vector<LogicalType>> &scanSchemas,
    vector<ExtentID> &target_eids,
    vector<vector<uint32_t>> &target_seqnos_per_extent,
    vector<idx_t> &mapping_idxs,
    vector<idx_t> &null_tuples_idx,
	vector<idx_t> &eid_to_mapping_idx, IOCache* io_cache)
{
    Catalog &cat_instance = client.db->GetCatalog();
	ExtentID prev_eid = std::numeric_limits<ExtentID>::max();
	Vector &src_vid_column_vector = input.data[nodeColIdx];
	target_eid_flags.reset();

	// Cursor initialization
	for (auto i = 0; i < target_seqnos_per_extent_map_cursors.size(); i++) {
		target_seqnos_per_extent_map_cursors[i] = 0;
	}
	boundary_position_cursor = 0;
	tmp_vec_cursor = 0;
	target_eids.clear();

	auto &validity = src_vid_column_vector.GetValidity();
	if (validity.AllValid()) {
		switch (src_vid_column_vector.GetVectorType()) {
			case VectorType::DICTIONARY_VECTOR: {
				for (size_t i = 0; i < input.size(); i++) {
					uint64_t vid = ((uint64_t *)src_vid_column_vector.GetData())[DictionaryVector::SelVector(src_vid_column_vector).get_index(i)];
					ExtentID target_eid = GET_EID_FROM_PHYSICAL_ID(vid);
					if (i == 0) prev_eid = target_eid;
					if (prev_eid != target_eid) {
						auto ext_seqno = GET_EXTENT_SEQNO_FROM_EID(prev_eid);
						target_eid_flags.set(ext_seqno, true);
						_fillTargetSeqnosVecAndBoundaryPosition(i, prev_eid);
					}
					tmp_vec[tmp_vec_cursor++] = i;
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
						auto ext_seqno = GET_EXTENT_SEQNO_FROM_EID(prev_eid);
						target_eid_flags.set(ext_seqno, true);
						_fillTargetSeqnosVecAndBoundaryPosition(i, prev_eid);
					}
					tmp_vec[tmp_vec_cursor++] = i;
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
						auto ext_seqno = GET_EXTENT_SEQNO_FROM_EID(prev_eid);
						target_eid_flags.set(ext_seqno, true);
						_fillTargetSeqnosVecAndBoundaryPosition(i, prev_eid);
					}
					tmp_vec[tmp_vec_cursor++] = i;
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
						auto ext_seqno = GET_EXTENT_SEQNO_FROM_EID(prev_eid);
						target_eid_flags.set(ext_seqno, true);
						_fillTargetSeqnosVecAndBoundaryPosition(i, prev_eid);
					}
					tmp_vec[tmp_vec_cursor++] = i;
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
						auto ext_seqno = GET_EXTENT_SEQNO_FROM_EID(prev_eid);
						target_eid_flags.set(ext_seqno, true);
						_fillTargetSeqnosVecAndBoundaryPosition(i, prev_eid);
					}
					tmp_vec[tmp_vec_cursor++] = i;
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
	if (tmp_vec_cursor > 0) {
		auto ext_seqno = GET_EXTENT_SEQNO_FROM_EID(prev_eid);
		target_eid_flags.set(ext_seqno, true);
		_fillTargetSeqnosVecAndBoundaryPosition(input.size(), prev_eid);
	}

	/**
	 * TODO: this code should be rmoved! this is temporal code
	 * Also, this is very slow due to GetEntry access.
	 * Optimize this.
	*/
	// remove extent ids to be removed due to filter
	// target_eids.reserve(INITIAL_EXTENT_ID_SPACE);
	auto partition_id = GET_PARTITION_ID_FROM_EID(prev_eid);
	for (auto i = 0; i < target_eid_flags.size(); i++) {
		if (target_eid_flags[i]) {
			auto eid = GET_EID_FROM_PARTITION_ID_AND_SEQNO(partition_id, i);
			auto ext_seqno = GET_EXTENT_SEQNO_FROM_EID(eid);
			if (eid_to_mapping_idx[ext_seqno] != -1) {
				target_eids.push_back(eid);
			}
		}
	}

	bool is_multi_schema = false;
	mapping_idxs.reserve(target_eids.size());
	for (auto i = 0; i < target_eids.size(); i++) {
		auto ext_seqno = GET_EXTENT_SEQNO_FROM_EID(target_eids[i]);
		idx_t mapping_idx = eid_to_mapping_idx[ext_seqno];
		D_ASSERT(mapping_idx != -1);
		mapping_idxs.push_back(mapping_idx);
		if (mapping_idx != mapping_idxs[0]) is_multi_schema = true;
		auto &vec = target_seqnos_per_extent_map[ext_seqno];
		auto cursor = target_seqnos_per_extent_map_cursors[ext_seqno];
		target_seqnos_per_extent.push_back({vec.begin(), vec.begin() + cursor});
	}

	// TODO maybe we don't need this..
	if (is_multi_schema) {
		ranges::sort(
			ranges::views::zip(mapping_idxs, target_eids, target_seqnos_per_extent), 
			std::less{}, 
			[](auto && p){ return std::get<0>(p); }
		);
	}

	ext_it->Initialize(client, scanSchemas, projection_mapping, &mapping_idxs, target_eids);

	return StoreAPIResult::OK;
}

StoreAPIResult iTbgppGraphStore::doVertexIndexSeek(
    ExtentIterator *&ext_it, DataChunk &output, DataChunk &input,
    idx_t nodeColIdx, std::vector<duckdb::LogicalType> &scanSchema,
    vector<ExtentID> &target_eids,
    vector<vector<uint32_t>> &target_seqnos_per_extent,
    vector<idx_t> &cols_to_include, idx_t current_pos,
    vector<idx_t> output_col_idx)
{
    if (ext_it == nullptr)
        return StoreAPIResult::DONE;
    ExtentID target_eid = target_eids[current_pos];
    ExtentID current_eid;
    D_ASSERT(ext_it != nullptr || ext_it->IsInitialized());
    D_ASSERT(current_pos < target_seqnos_per_extent.size());
    ext_it->GetNextExtent(
        client, output, current_eid, target_eid, input, nodeColIdx,
        output_col_idx, target_seqnos_per_extent[current_pos], cols_to_include);
    return StoreAPIResult::OK;
}

StoreAPIResult iTbgppGraphStore::doVertexIndexSeek(
    ExtentIterator *&ext_it, DataChunk &output, DataChunk &input,
    idx_t nodeColIdx, std::vector<duckdb::LogicalType> &scanSchema,
    vector<ExtentID> &target_eids,
    vector<vector<uint32_t>> &target_seqnos_per_extent, idx_t current_pos,
    idx_t out_id_col_idx, Vector &rowcol_vec, char *row_major_store, idx_t &num_output_tuples)
{
    ExtentID target_eid = target_eids[current_pos];
    ExtentID current_eid;
    D_ASSERT(ext_it != nullptr || ext_it->IsInitialized());
    D_ASSERT(current_pos < target_seqnos_per_extent.size());
    ext_it->GetNextExtentInRowFormat(
        client, output, current_eid, target_eid, input, nodeColIdx, rowcol_vec,
        row_major_store, target_seqnos_per_extent[current_pos], out_id_col_idx,
		num_output_tuples);
    return StoreAPIResult::OK;
}

StoreAPIResult iTbgppGraphStore::doVertexIndexSeek(
    ExtentIterator *&ext_it, DataChunk &output, DataChunk &input,
    idx_t nodeColIdx, std::vector<duckdb::LogicalType> &scanSchema,
    vector<ExtentID> &target_eids,
    vector<vector<uint32_t>> &target_seqnos_per_extent,
    vector<idx_t> &cols_to_include, idx_t current_pos,
    vector<idx_t> output_col_idx, idx_t &num_tuples_per_chunk)
{
    ExtentID target_eid = target_eids[current_pos];
    ExtentID current_eid;
    D_ASSERT(ext_it != nullptr || ext_it->IsInitialized());
    D_ASSERT(current_pos < target_seqnos_per_extent.size());
    ext_it->GetNextExtent(client, output, current_eid, target_eid, input,
                          nodeColIdx, output_col_idx,
                          target_seqnos_per_extent[current_pos],
                          cols_to_include, num_tuples_per_chunk);
    return StoreAPIResult::OK;
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
		adj_iter.getAdjListPtr(vid, target_eid, &start_ptr, &end_ptr, is_initialized);
	} else {
		adj_iter.getAdjListPtr(vid, target_eid, &start_ptr, &end_ptr, is_initialized);
	}
	prev_eid = target_eid;
	
	return StoreAPIResult::OK;
}


void iTbgppGraphStore::fillEidToMappingIdx(vector<uint64_t>& oids, vector<idx_t>& eid_to_mapping_idx, bool union_schema) {
	Catalog &cat_instance = client.db->GetCatalog();

	for (auto i = 0; i < oids.size(); i++) {
		auto oid = oids[i];
		PropertySchemaCatalogEntry* ps_cat_entry = 
			(PropertySchemaCatalogEntry*) cat_instance.GetEntry(client, DEFAULT_SCHEMA, oid);
		auto extent_ids = ps_cat_entry->extent_ids;

		for (auto eid: extent_ids) {
			auto ext_seqno = GET_EXTENT_SEQNO_FROM_EID(eid);
			if (ext_seqno > eid_to_mapping_idx.size()) {
				eid_to_mapping_idx.resize(ext_seqno + 1, -1);
			}
			eid_to_mapping_idx[ext_seqno] = union_schema ? 0 : i;
		}
	}
}

}