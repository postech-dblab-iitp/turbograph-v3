#include <cassert>
#include <algorithm>
#include <vector>
#include <boost/algorithm/string.hpp>

#include "typedef.hpp"

#include "storage/graph_store.hpp"

#include "livegraph.hpp"
#include "storage/livegraph_catalog.hpp"

#include "duckdb/common/vector_size.hpp"

LiveGraphStore::LiveGraphStore(livegraph::Graph& graph, LiveGraphCatalog& catalog) {
	this->graph = &graph;
	this->catalog = &catalog;
}

// TODO function for materializing adjlist

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
StoreAPIResult LiveGraphStore::doScan(duckdb::ChunkCollection& output, LabelSet labels, LoadAdjListOption loadAdj, PropertyKeys properties, std::vector<duckdb::LogicalType> scanSchema) {

	// assert if given chunkcollection is empty
	assert( output.Count() == 0 && "");	

	// start r.o. transaction
	auto txn = this->graph->begin_read_only_transaction();

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
	currentChunk->SetCardinality( (rowIdx) % 1024);
	output.Append(*currentChunk);
	
	// materialize adjlist
	if( loadAdj != LoadAdjListOption::NONE ) { 
		for( auto& range: rangesToScan) {
			for( long vid=range.first; vid <= range.second; vid++) {
				// TODO how to set adjlist??

				
				// get iterator
				// partition
				// insert into adjlist
				// make LogicalAdjList
			}
		}
	}

	// access properties and select indices
	if( properties.size() >= 0 ) {
		// assert there should be label
		assert( labels.size() > 0 && "label should be specified when accessing properties");
		std::vector<int> propertyIndices = getLDBCPropertyIndices(labels, properties);
		
		int attrOffsetShift = loadAdj == LoadAdjListOption::NONE ? 1 : 2;

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
