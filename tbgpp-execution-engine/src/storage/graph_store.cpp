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

StoreAPIResult iTbgppGraphStore::InitializeScan(std::queue<ExtentIterator *> &ext_its, vector<idx_t> &oids, vector<vector<uint64_t>> &projection_mapping,
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

StoreAPIResult iTbgppGraphStore::doScan(std::queue<ExtentIterator *> &ext_its, duckdb::DataChunk &output, vector<vector<uint64_t>> &projection_mapping, 
	std::vector<duckdb::LogicalType> &scanSchema, int64_t current_schema_idx) {
	ExtentID current_eid;
	auto ext_it = ext_its.front();
	bool scan_ongoing = ext_it->GetNextExtent(client, output, current_eid, projection_mapping[current_schema_idx]);
	printf("scan_ongoing ? %s, output.size = %ld, current_schema_idx = %ld\n", scan_ongoing ? "True" : "False", output.size(), current_schema_idx);
	if (scan_ongoing) {
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

StoreAPIResult iTbgppGraphStore::doScan(std::queue<ExtentIterator *> &ext_its, duckdb::DataChunk &output, vector<vector<uint64_t>> &projection_mapping, std::vector<duckdb::LogicalType> &scanSchema, int64_t &filterKeyColIdx, duckdb::Value &filterValue) {
	ExtentID current_eid;
	auto ext_it = ext_its.front();
	vector<idx_t> output_column_idxs; // TODO this should be generated in physical planning step // TODO s62 change me
	bool scan_ongoing = ext_it->GetNextExtent(client, output, current_eid, filterKeyColIdx, filterValue, projection_mapping[0], scanSchema); 
	if (scan_ongoing) {
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

StoreAPIResult iTbgppGraphStore::InitializeVertexIndexSeek(ExtentIterator *&ext_it, DataChunk& output, uint64_t vid, LabelSet labels, std::vector<LabelSet> &edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> &scanSchema) {
	D_ASSERT(ext_it == nullptr);
	Catalog &cat_instance = client.db->GetCatalog();
	D_ASSERT(labels.size() == 1); // XXX Temporary
	string entry_name = "vps_";
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

	ExtentID target_eid = vid >> 32; // TODO make this functionality as Macro --> GetEIDFromPhysicalID
	idx_t target_seqno = vid & 0x00000000FFFFFFFF; // TODO make this functionality as Macro --> GetSeqNoFromPhysicalID

	ext_it = new ExtentIterator();
	ext_it->Initialize(client, ps_cat_entry, scanSchema, column_idxs, target_eid);
	return StoreAPIResult::OK;
}

StoreAPIResult iTbgppGraphStore::InitializeVertexIndexSeek(ExtentIterator *&ext_it, DataChunk& output, DataChunk &input, idx_t nodeColIdx, LabelSet labels, std::vector<LabelSet> &edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> &scanSchema, vector<ExtentID> &target_eids, vector<idx_t> &boundary_position) {
	// if (ext_it != nullptr) delete ext_it;
	Catalog &cat_instance = client.db->GetCatalog();
	D_ASSERT(labels.size() == 1); // XXX Temporary
	string entry_name = "vps_";
	for (auto &it : labels.data) entry_name += it;
	PropertySchemaCatalogEntry* ps_cat_entry = 
      (PropertySchemaCatalogEntry*) cat_instance.GetEntry(client, CatalogType::PROPERTY_SCHEMA_ENTRY, DEFAULT_SCHEMA, entry_name);

	D_ASSERT(edgeLabels.size() <= 1); // XXX Temporary
	vector<string> properties_temp;
	for (size_t i = 0; i < edgeLabels.size(); i++) {
		for (const auto &it : edgeLabels[i].data) properties_temp.push_back(it);
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

	// std::sort( target_eids.begin(), target_eids.end() );
	target_eids.erase( std::unique( target_eids.begin(), target_eids.end() ), target_eids.end() );
	// icecream::ic.enable(); IC(target_eids.size(), boundary_position.size()); 
	// for (size_t i = 0; i < target_eids.size(); i++) IC(i, target_eids[i]);
	// icecream::ic.disable();

	if (target_eids.size() == 0) return StoreAPIResult::DONE;
// icecream::ic.enable(); IC(); icecream::ic.disable();
	ext_it = new ExtentIterator();
	ext_it->Initialize(client, ps_cat_entry, scanSchema, column_idxs, target_eids);
// icecream::ic.enable(); IC(); icecream::ic.disable();
	return StoreAPIResult::OK;
}

StoreAPIResult iTbgppGraphStore::InitializeVertexIndexSeek(std::queue<ExtentIterator *> &ext_its, vector<idx_t> &oids, vector<vector<uint64_t>> &projection_mapping, DataChunk &input, idx_t nodeColIdx, std::vector<duckdb::LogicalType> &scanSchema, vector<ExtentID> &target_eids, vector<idx_t> &boundary_position) {
	Catalog &cat_instance = client.db->GetCatalog();

	D_ASSERT(oids.size() == 1); // temporary
	D_ASSERT(oids.size() == projection_mapping.size());
	
	vector<vector<idx_t>> column_idxs;
	for (idx_t i = 0; i < oids.size(); i++) {
		PropertySchemaCatalogEntry* ps_cat_entry = 
      		(PropertySchemaCatalogEntry*) cat_instance.GetEntry(client, DEFAULT_SCHEMA, oids[i]);
// icecream::ic.enable();
// 		IC();
// 		IC(i, oids[i], ps_cat_entry->GetName());
// 		for (int j = 0; j < scanSchema.size(); j++) {
// 			IC((uint8_t)scanSchema[j].id(), projection_mapping[i][j]);
// 		}
// icecream::ic.disable();

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

		auto ext_it = new ExtentIterator();
		ext_it->Initialize(client, ps_cat_entry, scanSchema, projection_mapping[i], target_eids);
		ext_its.push(ext_it);
	}

	return StoreAPIResult::OK;
}

StoreAPIResult iTbgppGraphStore::doVertexIndexSeek(ExtentIterator *&ext_it, DataChunk& output, uint64_t vid, LabelSet labels, std::vector<LabelSet> &edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> &scanSchema) {
	D_ASSERT(false);
	D_ASSERT(ext_it != nullptr || ext_it->IsInitialized());
	ExtentID target_eid = vid >> 32; // TODO make this functionality as Macro --> GetEIDFromPhysicalID
	idx_t target_seqno = vid & 0x00000000FFFFFFFF; // TODO make this functionality as Macro --> GetSeqNoFromPhysicalID
	ExtentID current_eid;
	bool scan_ongoing = ext_it->GetNextExtent(client, output, current_eid, target_eid, target_seqno);

	// IC(vid, target_eid, target_seqno, current_eid);
	if (scan_ongoing) assert(current_eid == target_eid);
	
	return StoreAPIResult::OK;
}

StoreAPIResult iTbgppGraphStore::doVertexIndexSeek(ExtentIterator *&ext_it, DataChunk& output, DataChunk &input, idx_t nodeColIdx, LabelSet labels, std::vector<LabelSet> &edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> &scanSchema, vector<ExtentID> &target_eids, vector<idx_t> &boundary_position, idx_t current_pos, vector<idx_t> output_col_idx) {
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

StoreAPIResult iTbgppGraphStore::doVertexIndexSeek(std::queue<ExtentIterator *> &ext_its, DataChunk& output, DataChunk &input,
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

StoreAPIResult iTbgppGraphStore::doVertexIndexSeek(std::queue<ExtentIterator *> &ext_its, DataChunk& output, DataChunk &input,
												   idx_t nodeColIdx, std::vector<duckdb::LogicalType> &scanSchema, vector<ExtentID> &target_eids,
												   vector<idx_t> &boundary_position, idx_t current_pos, vector<idx_t> output_col_idx,
												   idx_t &output_idx, SelectionVector &sel, int64_t &filterKeyColIdx, duckdb::Value &filterValue) {
	ExtentID target_eid = target_eids[current_pos]; // TODO make this functionality as Macro --> GetEIDFromPhysicalID
	ExtentID current_eid;
	auto ext_it = ext_its.front();
	D_ASSERT(ext_it != nullptr || ext_it->IsInitialized());
	if (current_pos >= boundary_position.size()) throw InvalidInputException("??");
	idx_t start_seqno, end_seqno; // [start_seqno, end_seqno]
	start_seqno = current_pos == 0 ? 0 : boundary_position[current_pos - 1] + 1;
	end_seqno = boundary_position[current_pos];
	bool scan_ongoing = ext_it->GetNextExtent(client, output, current_eid,
											  filterKeyColIdx, filterValue, target_eid, input,
											  nodeColIdx, output_col_idx, start_seqno, end_seqno,
											  output_idx, sel);

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

StoreAPIResult iTbgppGraphStore::InitializeEdgeIndexSeek(ExtentIterator *&ext_it, DataChunk& output, uint64_t vid, LabelSet labels, std::vector<LabelSet> &edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> &scanSchema) {
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
	ext_it->Initialize(client, ps_cat_entry, scanSchema, column_idxs, target_eid);
	return StoreAPIResult::OK;
}

StoreAPIResult iTbgppGraphStore::InitializeEdgeIndexSeek(ExtentIterator *&ext_it, DataChunk& output, DataChunk &input, idx_t nodeColIdx, LabelSet labels, std::vector<LabelSet> &edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> &scanSchema, vector<ExtentID> &target_eids, vector<idx_t> &boundary_position) {
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
	ext_it->Initialize(client, ps_cat_entry, scanSchema, column_idxs, target_eids);
	return StoreAPIResult::OK;
}

StoreAPIResult iTbgppGraphStore::doEdgeIndexSeek(ExtentIterator *&ext_it, DataChunk& output, DataChunk &input, idx_t nodeColIdx, LabelSet labels, std::vector<LabelSet> &edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> &scanSchema, vector<ExtentID> &target_eids, vector<idx_t> &boundary_position, idx_t current_pos, vector<idx_t> output_col_idx) {
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

bool iTbgppGraphStore::isNodeInLabelset(u_int64_t id, LabelSet labels) {
	return true;
}

void iTbgppGraphStore::getAdjColIdxs(idx_t index_cat_oid, vector<int> &adjColIdxs, vector<LogicalType> &adjColTypes) {
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

// TODO deprecate this API
void iTbgppGraphStore::getAdjColIdxs(LabelSet src_labels, LabelSet edge_labels, ExpandDirection expand_dir, vector<int> &adjColIdxs, vector<LogicalType> &adjColTypes) {
	Catalog &cat_instance = client.db->GetCatalog();
	D_ASSERT(src_labels.size() == 1); // XXX Temporary
	if( src_labels.size()!= 1 ) {
		throw InvalidInputException("demo08 invalid!");
	}
// IC();
	string entry_name = "vps_";
	for (auto &it : src_labels.data) entry_name += it;
// icecream::ic.enable(); IC(); IC(entry_name); icecream::ic.disable();
	PropertySchemaCatalogEntry* ps_cat_entry = 
      (PropertySchemaCatalogEntry*) cat_instance.GetEntry(client, CatalogType::PROPERTY_SCHEMA_ENTRY, DEFAULT_SCHEMA, entry_name);

	LogicalTypeId_vector *l_types = ps_cat_entry->GetTypes();
	string_vector *keys = ps_cat_entry->GetKeys();
	D_ASSERT(l_types->size() == keys->size());
// icecream::ic.enable(); for( auto& key : keys) { IC(key); } icecream::ic.disable();
// icecream::ic.enable(); IC(l_types.size(), keys.size()); icecream::ic.disable();
	if (expand_dir == ExpandDirection::OUTGOING) {
		for (int i = 0; i < l_types->size(); i++)
			if (((*l_types)[i] == LogicalType::FORWARD_ADJLIST) && edge_labels.contains(std::string((*keys)[i]))) {
				adjColIdxs.push_back(i);
				adjColTypes.push_back(LogicalType::FORWARD_ADJLIST);
			}
	} else if (expand_dir == ExpandDirection::INCOMING) {
		for (int i = 0; i < l_types->size(); i++)
			if (((*l_types)[i] == LogicalType::BACKWARD_ADJLIST) && edge_labels.contains(std::string((*keys)[i]))) {
				adjColIdxs.push_back(i);
				adjColTypes.push_back(LogicalType::BACKWARD_ADJLIST);
			}
	} else if (expand_dir == ExpandDirection::BOTH) {
		for (int i = 0; i < l_types->size(); i++) {
			if (edge_labels.contains(std::string((*keys)[i]))) {
				if ((*l_types)[i] == LogicalType::FORWARD_ADJLIST) adjColTypes.push_back(LogicalType::FORWARD_ADJLIST);
				else if ((*l_types)[i] == LogicalType::BACKWARD_ADJLIST) adjColTypes.push_back(LogicalType::BACKWARD_ADJLIST);
				adjColIdxs.push_back(i);
			}
		}
	}

}

StoreAPIResult iTbgppGraphStore::getAdjListRange(AdjacencyListIterator &adj_iter, int adjColIdx, uint64_t vid, uint64_t* start_idx, uint64_t* end_idx) {
	D_ASSERT(false);
	adj_iter.Initialize(client, adjColIdx, vid);
	adj_iter.getAdjListRange(vid, start_idx, end_idx);
	return StoreAPIResult::OK; 
}

StoreAPIResult iTbgppGraphStore::getAdjListFromRange(AdjacencyListIterator &adj_iter, int adjColIdx, uint64_t vid, uint64_t start_idx, uint64_t end_idx, duckdb::DataChunk& output, idx_t *&adjListBase) {
	D_ASSERT(false);
	adj_iter.Initialize(client, adjColIdx, vid);
	// adj_iter.getAdjListRange(vid, start_idx, end_idx);
	return StoreAPIResult::OK;
}

StoreAPIResult iTbgppGraphStore::getAdjListFromVid(AdjacencyListIterator &adj_iter, int adjColIdx, uint64_t vid, uint64_t *&start_ptr, uint64_t *&end_ptr, ExpandDirection expand_dir) {
	D_ASSERT( expand_dir ==ExpandDirection::OUTGOING || expand_dir == ExpandDirection::INCOMING );
	bool is_initialized;
	ExtentID target_eid = vid >> 32;
	if (expand_dir == ExpandDirection::OUTGOING) {
		// icecream::ic.enable(); IC(); IC(adjColIdx, vid, target_eid); icecream::ic.disable();
		is_initialized = adj_iter.Initialize(client, adjColIdx, target_eid, LogicalType::FORWARD_ADJLIST);
		// icecream::ic.enable(); IC(); icecream::ic.disable();
	} else if (expand_dir == ExpandDirection::INCOMING) {
		// icecream::ic.enable(); IC(); IC(adjColIdx, vid, target_eid); icecream::ic.disable();
		is_initialized = adj_iter.Initialize(client, adjColIdx, target_eid, LogicalType::BACKWARD_ADJLIST);
		// icecream::ic.enable(); IC(); icecream::ic.disable();
	}

	adj_iter.getAdjListPtr(vid, target_eid, start_ptr, end_ptr, is_initialized);
	return StoreAPIResult::OK;
}

}