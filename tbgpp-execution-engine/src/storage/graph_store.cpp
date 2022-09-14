#include <cassert>
#include <algorithm>
#include <vector>
#include <boost/algorithm/string.hpp>

#include "typedef.hpp"

#include "storage/graph_store.hpp"

//#include "livegraph.hpp"
#include "storage/livegraph_catalog.hpp"

#include "common/vector_size.hpp"
#include "main/database.hpp"
#include "main/client_context.hpp"
#include "catalog/catalog.hpp"
#include "extent/extent_iterator.hpp"
#include "catalog/catalog_entry/list.hpp"

#include "icecream.hpp"

namespace duckdb {
/*LiveGraphStore::LiveGraphStore(livegraph::Graph* graph, LiveGraphCatalog* catalog) {
	this->graph = graph;
	this->catalog = catalog;
}*/

iTbgppGraphStore::iTbgppGraphStore(ClientContext &client) : client(client) {

}

StoreAPIResult iTbgppGraphStore::InitializeScan(ExtentIterator *&ext_it, LabelSet labels, std::vector<LabelSet> edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> scanSchema) {
	Catalog &cat_instance = client.db->GetCatalog();
	D_ASSERT(labels.size() == 1); // XXX Temporary
	string entry_name = "vps_";
	for (auto &it : labels.data) entry_name += it;
	PropertySchemaCatalogEntry* ps_cat_entry = 
      (PropertySchemaCatalogEntry*) cat_instance.GetEntry(client, CatalogType::PROPERTY_SCHEMA_ENTRY, "main", entry_name);
	D_ASSERT(edgeLabels.size() <= 1); // XXX Temporary
	vector<string> properties_temp;
	for (size_t i = 0; i < edgeLabels.size(); i++) {
		for (auto &it : edgeLabels[i].data) properties_temp.push_back(it);
	}
	for (auto &it : properties) properties_temp.push_back(it);
	vector<idx_t> column_idxs;
	column_idxs = move(ps_cat_entry->GetColumnIdxs(properties_temp));

	ext_it = new ExtentIterator();
	ext_it->Initialize(client, ps_cat_entry, scanSchema, column_idxs);
	return StoreAPIResult::OK;
}

StoreAPIResult iTbgppGraphStore::doScan(ExtentIterator *&ext_it, DataChunk& output, LabelSet labels, std::vector<LabelSet> edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> scanSchema) {
	
	ExtentID current_eid;
	bool scan_ongoing = ext_it->GetNextExtent(client, output, current_eid);
	if (scan_ongoing) {
		//output.Reference(*output_);
		return StoreAPIResult::OK;
	} else return StoreAPIResult::DONE;
}

StoreAPIResult iTbgppGraphStore::doScan(ExtentIterator *&ext_it, DataChunk& output, LabelSet labels, std::vector<LabelSet> edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> scanSchema, std::string filterKey, duckdb::Value filterValue) {
	ExtentID current_eid;
	D_ASSERT(edgeLabels.size() <= 1); // XXX Temporary
	D_ASSERT(filterKey.compare("") != 0);
	vector<string> output_properties;
	for (size_t i = 0; i < edgeLabels.size(); i++) {
		for (auto &it : edgeLabels[i].data) output_properties.push_back(it);
	}
	for (auto &it : properties) output_properties.push_back(it);

	bool scan_ongoing = ext_it->GetNextExtent(client, output, current_eid, filterKey, filterValue, output_properties, scanSchema);
	if (scan_ongoing) {
		//output.Reference(*output_);
		return StoreAPIResult::OK;
	} else return StoreAPIResult::DONE;
}

StoreAPIResult iTbgppGraphStore::doIndexSeek(ExtentIterator *&ext_it, DataChunk& output, uint64_t vid, LabelSet labels, std::vector<LabelSet> edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> scanSchema) {
	Catalog &cat_instance = client.db->GetCatalog();
	D_ASSERT(labels.size() == 1); // XXX Temporary
	string entry_name = "vps_";
	for (auto &it : labels.data) entry_name += it;
	PropertySchemaCatalogEntry* ps_cat_entry = 
      (PropertySchemaCatalogEntry*) cat_instance.GetEntry(client, CatalogType::PROPERTY_SCHEMA_ENTRY, "main", entry_name);

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

	ExtentID current_eid;
	bool scan_ongoing = ext_it->GetNextExtent(client, output, current_eid, target_seqno);
	D_ASSERT(current_eid == target_eid);
	scan_ongoing = ext_it->GetNextExtent(client, output, current_eid, target_seqno);
	D_ASSERT(scan_ongoing == false);
	return StoreAPIResult::OK;
}

StoreAPIResult iTbgppGraphStore::doEdgeIndexSeek(ExtentIterator *&ext_it, DataChunk& output, uint64_t vid, LabelSet labels, std::vector<LabelSet> edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> scanSchema) {
	Catalog &cat_instance = client.db->GetCatalog();
	D_ASSERT(labels.size() == 1); // XXX Temporary
	// std::cout << "A\n";
	string entry_name = "eps_";
	for (auto &it : labels.data) entry_name += it;
	PropertySchemaCatalogEntry* ps_cat_entry = 
      (PropertySchemaCatalogEntry*) cat_instance.GetEntry(client, CatalogType::PROPERTY_SCHEMA_ENTRY, "main", entry_name);
	D_ASSERT(edgeLabels.size() <= 1); // XXX Temporary
	// std::cout << "B\n";
	vector<string> properties_temp;
	for (size_t i = 0; i < edgeLabels.size(); i++) {
		for (auto &it : edgeLabels[i].data) properties_temp.push_back(it);
	}
	// std::cout << "C\n";
	for (auto &it : properties) {
		// std::cout << "Property: " << it << std::endl;
		properties_temp.push_back(it);
	}
	vector<idx_t> column_idxs;
	// std::cout << "D\n";
	column_idxs = move(ps_cat_entry->GetColumnIdxs(properties_temp));

	ExtentID target_eid = vid >> 32; // TODO make this functionality as Macro --> GetEIDFromPhysicalID
	idx_t target_seqno = vid & 0x00000000FFFFFFFF; // TODO make this functionality as Macro --> GetSeqNoFromPhysicalID
	ext_it = new ExtentIterator();
	// std::cout << "E\n";
	ext_it->Initialize(client, ps_cat_entry, scanSchema, column_idxs, target_eid);

	ExtentID current_eid;
	// std::cout << "F\n";
	bool scan_ongoing = ext_it->GetNextExtent(client, output, current_eid, target_seqno);
	D_ASSERT(current_eid == target_eid);
	// std::cout << "G\n";
	scan_ongoing = ext_it->GetNextExtent(client, output, current_eid, target_seqno);
	D_ASSERT(scan_ongoing == false);
	return StoreAPIResult::OK;
}

bool iTbgppGraphStore::isNodeInLabelset(u_int64_t id, LabelSet labels) {
	return true;
}

void iTbgppGraphStore::getAdjColIdxs(LabelSet labels, vector<int> &adjColIdxs, ExpandDirection expand_dir, LabelSet edgeLabels) {
	Catalog &cat_instance = client.db->GetCatalog();
	D_ASSERT(labels.size() == 1); // XXX Temporary
	if( labels.size()!= 1 ) {
		throw InvalidInputException("demo08 invalid!");
	}
// IC();
	string entry_name = "vps_";
	for (auto &it : labels.data) entry_name += it;
	PropertySchemaCatalogEntry* ps_cat_entry = 
      (PropertySchemaCatalogEntry*) cat_instance.GetEntry(client, CatalogType::PROPERTY_SCHEMA_ENTRY, "main", entry_name);

	vector<LogicalType> l_types = move(ps_cat_entry->GetTypes());
	vector<string> keys = move(ps_cat_entry->GetKeys());
	D_ASSERT(l_types.size() == keys.size());
// for( auto& key : keys) { IC(key); }
	if (expand_dir == ExpandDirection::OUTGOING) {
		for (int i = 0; i < l_types.size(); i++)
			if ((l_types[i] == LogicalType::FORWARD_ADJLIST) && edgeLabels.contains(keys[i])) adjColIdxs.push_back(i);
	} else if (expand_dir == ExpandDirection::INCOMING) {
		for (int i = 0; i < l_types.size(); i++)
			if ((l_types[i] == LogicalType::BACKWARD_ADJLIST) && edgeLabels.contains(keys[i])) adjColIdxs.push_back(i);
	} else if (expand_dir == ExpandDirection::BOTH) {
		for (int i = 0; i < l_types.size(); i++)
			if ((l_types[i] == LogicalType::FORWARD_ADJLIST || l_types[i] == LogicalType::BACKWARD_ADJLIST) && edgeLabels.contains(keys[i])) adjColIdxs.push_back(i);
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
	if (expand_dir == ExpandDirection::OUTGOING) {
		adj_iter.Initialize(client, adjColIdx, vid, LogicalType::FORWARD_ADJLIST);
	} else if (expand_dir == ExpandDirection::INCOMING) {
		adj_iter.Initialize(client, adjColIdx, vid, LogicalType::BACKWARD_ADJLIST);
	}

	adj_iter.getAdjListPtr(vid, start_ptr, end_ptr);
	return StoreAPIResult::OK;
}

// This should be eliminated anytime soon.
std::vector<int> getLDBCPropertyIndices(LabelSet labels, PropertyKeys properties) {

	std::vector<int> result;
	
	if( labels.contains("Comment") ) {
		std::vector<std::string> schema({"id", "creationDate", "locationIP", "browserUsed", "content", "length"});
		for( auto& prop: properties) {
			int idx = std::distance(schema.begin(), (std::find(schema.begin(), schema.end(), prop)) );
			result.push_back( idx );
		}
	} else if( labels.contains("Forum")) {
		std::vector<std::string> schema({"id", "title", "creationDate"});
		for( auto& prop: properties) {
			int idx = std::distance(schema.begin(), (std::find(schema.begin(), schema.end(), prop)) );
			result.push_back( idx );
		}
	} else if( labels.contains("Person")) {
		std::vector<std::string> schema({"id", "firstName", "lastName", "gender", "birthday", "creationDate", "locationIP", "browserUsed", "speaks", "email"});
		for( auto& prop: properties) {
			int idx = std::distance(schema.begin(), (std::find(schema.begin(), schema.end(), prop)) );
			result.push_back( idx );
		}
	} else if( labels.contains("Post")) {
		std::vector<std::string> schema({"id", "imageFile", "creationDate", "locationIP", "browserUsed", "language", "content", "length"});
		for( auto& prop: properties) {
			int idx = std::distance(schema.begin(), (std::find(schema.begin(), schema.end(), prop)) );
			result.push_back( idx );
		}
	} else if( labels.contains("TagClass")) {
		std::vector<std::string> schema({"id", "name", "url"});
		for( auto& prop: properties) {
			int idx = std::distance(schema.begin(), (std::find(schema.begin(), schema.end(), prop)) );
			result.push_back( idx );
		}
	} else if( labels.contains("Tag")) {
		std::vector<std::string> schema({"id", "name", "url"});
		for( auto& prop: properties) {
			int idx = std::distance(schema.begin(), (std::find(schema.begin(), schema.end(), prop)) );
			result.push_back( idx );
		}
	} else if( labels.contains("City") || labels.contains("Country") || labels.contains("Continent") || labels.contains("Place")) {
		std::vector<std::string> schema({"id", "name", "url"});
		for( auto& prop: properties) {
			int idx = std::distance(schema.begin(), (std::find(schema.begin(), schema.end(), prop)) );
			result.push_back( idx );
		}
	} else if( labels.contains("Company") || labels.contains("University") || labels.contains("Organisation") ) {
		std::vector<std::string> schema({"id", "name", "url"});
		for( auto& prop: properties) {
			int idx = std::distance(schema.begin(), (std::find(schema.begin(), schema.end(), prop)) );
			result.push_back( idx );
		}
	}
	assert( result.size() == properties.size() );
	return result;
}

// APIs
/*StoreAPIResult LiveGraphStore::doScan(duckdb::ChunkCollection& output, LabelSet labels, std::vector<LabelSet> edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> scanSchema) {
	// assert if given chunkcollection is empty
	assert( output.Count() == 0 && "");	
	// start r.o. transaction
	auto txn = this->graph->begin_read_only_transaction();

	// offset shift function
	auto getAttrOffsetShift = [&]{
		int result;
		if (loadAdj == LoadAdjListOption::NONE) { result = 1; }
		else if (loadAdj == LoadAdjListOption::BOTH) { result = 1 + int(edgeLabels.size())*2; }
		else { result =  1 + edgeLabels.size(); }
		return result;
	};
	// access catalog and find all ranges satisfying labels
	std::vector<std::pair<long, long>> rangesToScan;
	for( auto& item: this->catalog->vertexLabelSetToLGRangeMapping ) {
		auto ls = item.first;
		if(ls.isSupersetOf(labels)) {
			rangesToScan.push_back(item.second);
		}
	}
	// return if nothing to scan
	if( rangesToScan.size() == 0 ) { return StoreAPIResult::OK; }
	// insert vids
	duckdb::DataChunk* currentChunk;
	long rowIdx = 0 ;
	for(auto& range: rangesToScan) {
		for(long vid=range.first; vid <= range.second; vid++) {
			if( rowIdx % 1024 == 0 ) {
				if(rowIdx != 0) {
					// first append filled chunk
					currentChunk->SetCardinality(1024);
					output.Append(*currentChunk);
				}
				// generate new chunk
				// std::cout << vid << " " << rowIdx << std::endl;
				currentChunk = new duckdb::DataChunk();
				currentChunk->Initialize(scanSchema);
			}
			currentChunk->SetValue(0, rowIdx % 1024, duckdb::Value::UBIGINT(vid) );
			rowIdx += 1;
		}
	}
	// finally append last chunk
	currentChunk->SetCardinality( (rowIdx) % 1024 );
	output.Append(*currentChunk);

	// materialize adjlist
	if( loadAdj != LoadAdjListOption::NONE ) { 
		std::vector<long> livegraphEdgeLabels;
		for( auto& el: edgeLabels ) {
			assert( (el.size() == 1) && "Currently support one edge labels" ) ; 
			for( auto& item: catalog->edgeLabelSetToLGEdgeLabelMapping ) {
				if( el == item.first) {
					livegraphEdgeLabels.push_back(item.second);
					break;
				}
			}
		}
		assert( livegraphEdgeLabels.size() == edgeLabels.size());

		rowIdx = 0;
		for( auto& range: rangesToScan) {
			for( long vid=range.first; vid <= range.second; vid++) {
				
				for( int adjIdx = 0; adjIdx < livegraphEdgeLabels.size(); adjIdx++) {

					livegraph::EdgeIterator outgoingEdgeIt = txn.get_edges(vid, livegraphEdgeLabels[adjIdx], false); // outgoing
					// TODO need to find reverse adjacency list
					// The reverse option does not mean it is incoming or outgoing, just iterator ordering
					assert( (loadAdj != LoadAdjListOption::INCOMING) && "+need to build incoming adjlist" );
					assert( (loadAdj != LoadAdjListOption::BOTH) && "+need to build incoming adjlist" );
					// 
					livegraph::EdgeIterator incomingEdgeIt = txn.get_edges(vid, livegraphEdgeLabels[adjIdx], true); // incoming
					// make list of edges
					std::vector<duckdb::Value> outgoingEdges;
					std::vector<duckdb::Value> incomingEdges;
					// iterate
					while( outgoingEdgeIt.valid() ) {
						outgoingEdges.push_back( duckdb::Value::UBIGINT(outgoingEdgeIt.dst_id()) );
						outgoingEdgeIt.next();
					}
					while( incomingEdgeIt.valid() ) {
						incomingEdges.push_back( duckdb::Value::UBIGINT(incomingEdgeIt.dst_id()) );
						incomingEdgeIt.next();
					}
					
					switch( loadAdj ) {
						case LoadAdjListOption::OUTGOING:
							output.SetValue( 1 + adjIdx, rowIdx, duckdb::Value::LIST(duckdb::LogicalType::UBIGINT, outgoingEdges) );
							break;
						case LoadAdjListOption::INCOMING:
							output.SetValue( 1 + adjIdx, rowIdx, duckdb::Value::LIST(duckdb::LogicalType::UBIGINT, incomingEdges) );
							break;
						case LoadAdjListOption::BOTH:
							output.SetValue( 1 + adjIdx, rowIdx, duckdb::Value::LIST(duckdb::LogicalType::UBIGINT, outgoingEdges) );
							output.SetValue( 1 + livegraphEdgeLabels.size() + adjIdx, rowIdx, duckdb::Value::LIST(duckdb::LogicalType::UBIGINT, outgoingEdges) );
							break;
					}
				}
				rowIdx += 1;
			}
		}
	}
	// access properties and select indices
	if( properties.size() >= 0 ) {
		// assert there should be label
		assert( labels.size() > 0 && "label should be specified when accessing properties");
		std::vector<int> propertyIndices = getLDBCPropertyIndices(labels, properties);
	
		int attrOffsetShift = getAttrOffsetShift();

		rowIdx = 0;
		for(auto& range: rangesToScan) {
			for(long vid=range.first; vid <= range.second; vid++) {
				std::string rawVertexProp = std::string(txn.get_vertex(vid));
				std::vector<std::string> props;
				boost::split( props, rawVertexProp, boost::is_any_of("|") );

				for( int attrIdx = 0; attrIdx < propertyIndices.size(); attrIdx++) {
					auto propType = scanSchema[attrIdx+attrOffsetShift];
					auto propIdx = propertyIndices[attrIdx];
					assert(propIdx < props.size());

					if( propType == duckdb::LogicalType::BIGINT) {
						output.SetValue(attrIdx+attrOffsetShift, rowIdx, duckdb::Value::BIGINT(std::stol(props[propIdx])) );
					} else if ( propType == duckdb::LogicalType::VARCHAR) {
						output.SetValue(attrIdx+attrOffsetShift, rowIdx, duckdb::Value(props[propIdx]) );
					} else {
						assert( false && "unsupported property type");
					}		
				}
				rowIdx +=1;
			}
		}
	}

	return StoreAPIResult::OK;
}

StoreAPIResult LiveGraphStore::doIndexSeek(duckdb::DataChunk& output, uint64_t vid, LabelSet labels, std::vector<LabelSet> edgeLabels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> scanSchema){

	assert( output.size() == 0 && "");	
	auto txn = this->graph->begin_read_only_transaction();

	// offset shift function
	auto getAttrOffsetShift = [&]{
		int result;
		if (loadAdj == LoadAdjListOption::NONE) { result = 1; }
		else if (loadAdj == LoadAdjListOption::BOTH) { result = 1 + int(edgeLabels.size())*2; }
		else { result =  1 + edgeLabels.size(); }
		return result;
	};
	// add vid
	output.SetValue(0, 0, duckdb::Value::UBIGINT(vid) );
	// add adj
	if( loadAdj != LoadAdjListOption::NONE ) { 
		std::vector<long> livegraphEdgeLabels;
		for( auto& el: edgeLabels ) {
			assert( (el.size() == 1) && "Currently support one edge labels" ) ; 
			for( auto& item: catalog->edgeLabelSetToLGEdgeLabelMapping ) {
				if( el == item.first) {
					livegraphEdgeLabels.push_back(item.second);
					break;
				}
			}
		}
		assert( livegraphEdgeLabels.size() == edgeLabels.size());
		for( int adjIdx = 0; adjIdx < livegraphEdgeLabels.size(); adjIdx++) {

			livegraph::EdgeIterator outgoingEdgeIt = txn.get_edges(vid, livegraphEdgeLabels[adjIdx], false); // outgoing
			// TODO need to find reverse adjacency list
			// The reverse option does not mean it is incoming or outgoing, just iterator ordering
			assert( (loadAdj != LoadAdjListOption::INCOMING) && "+need to build incoming adjlist" );
			assert( (loadAdj != LoadAdjListOption::BOTH) && "+need to build incoming adjlist" );
			// 
			livegraph::EdgeIterator incomingEdgeIt = txn.get_edges(vid, livegraphEdgeLabels[adjIdx], true); // incoming
			// make list of edges
			std::vector<duckdb::Value> outgoingEdges;
			std::vector<duckdb::Value> incomingEdges;
			// iterate
			while( outgoingEdgeIt.valid() ) {
				outgoingEdges.push_back( duckdb::Value::UBIGINT(outgoingEdgeIt.dst_id()) );
				outgoingEdgeIt.next();
			}
			while( incomingEdgeIt.valid() ) {
				incomingEdges.push_back( duckdb::Value::UBIGINT(incomingEdgeIt.dst_id()) );
				incomingEdgeIt.next();
			}
			
			switch( loadAdj ) {
				case LoadAdjListOption::OUTGOING:
					output.SetValue( 1 + adjIdx, 0, duckdb::Value::LIST(duckdb::LogicalType::UBIGINT, outgoingEdges) );
					break;
				case LoadAdjListOption::INCOMING:
					output.SetValue( 1 + adjIdx, 0, duckdb::Value::LIST(duckdb::LogicalType::UBIGINT, incomingEdges) );
					break;
				case LoadAdjListOption::BOTH:
					output.SetValue( 1 + adjIdx, 0, duckdb::Value::LIST(duckdb::LogicalType::UBIGINT, outgoingEdges) );
					output.SetValue( 1 + livegraphEdgeLabels.size() + adjIdx, 0, duckdb::Value::LIST(duckdb::LogicalType::UBIGINT, outgoingEdges) );
					break;
			}
		}	
	}
	// add properties
	if( properties.size() >= 0 ) {
		// assert there should be label
		assert( labels.size() > 0 && "label should be specified when accessing properties");
		std::vector<int> propertyIndices = getLDBCPropertyIndices(labels, properties);
	
		int attrOffsetShift = getAttrOffsetShift();

		std::string rawVertexProp = std::string(txn.get_vertex(vid));
		std::vector<std::string> props;
		boost::split( props, rawVertexProp, boost::is_any_of("|") );

		for( int attrIdx = 0; attrIdx < propertyIndices.size(); attrIdx++) {
			auto propType = scanSchema[attrIdx+attrOffsetShift];
			auto propIdx = propertyIndices[attrIdx];
			assert(propIdx < props.size());

			if( propType == duckdb::LogicalType::BIGINT) {
				output.SetValue(attrIdx+attrOffsetShift, 0, duckdb::Value::BIGINT(std::stol(props[propIdx])) );
			} else if ( propType == duckdb::LogicalType::VARCHAR) {
				output.SetValue(attrIdx+attrOffsetShift, 0, duckdb::Value(props[propIdx]) );
			} else {
				assert( false && "unsupported property type");
			}		
		}
	}
	output.SetCardinality(1);
	return StoreAPIResult::OK;
}

bool LiveGraphStore::isNodeInLabelset(uint64_t vid, LabelSet labels) {
	// if input is superset of ls, check id range and return true.
	for( auto& item: this->catalog->vertexLabelSetToLGRangeMapping ) {
		auto ls = item.first;
		if(ls.isSupersetOf(labels)) {
			// find range
			if( item.second.first <= vid && item.second.second >= vid) {
				return true;
			}
		}
	}
	return false;
}*/
}