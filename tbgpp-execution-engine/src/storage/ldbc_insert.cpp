#include <iostream>
#include <cassert> 
#include <filesystem>
#include <algorithm>
#include <fstream>
#include <string>
#include <map>
#include <cctype>
#include <climits>

#include "typedef.hpp"

#include "livegraph.hpp"
#include "storage/ldbc_insert.hpp"
#include "storage/livegraph_catalog.hpp"


namespace fs = std::filesystem;

void strConvertToVertexLabelFormat(std::string& input) {

	// first, all letters to lower
	std::transform(input.begin(), input.end(), input.begin(),
		[](unsigned char c){ return std::tolower(c); });
	// first letter upper
	std::transform(input.begin(), input.begin()+1, input.begin(),
		[](unsigned char c){ return std::toupper(c); });
	if ( input.compare("Tagclass") == 0) {
		input = "TagClass";
	} 
}

void strConvertToEdgeLabelFormat(std::string& input) {

	// isLocatedIn -> IS_LOCATED_IN
	std::string tmp = input;
	input = "";
	for(auto c: tmp) {
		if(islower(c)) {
			input += toupper(c);
		}
		if(isupper(c)) {
			input += '_';
			input += c;
		}
	}
	
}

void LDBCInsert(livegraph::Graph& graph, LiveGraphCatalog& catalog,  std::string ldbc_path) { // TODO insert ldbc directory

	// start transaction
	livegraph::Transaction txn = graph.begin_batch_loader();

	// read all files
	std::string dyn_path = ldbc_path + "/dynamic/";
	std::string stat_path = ldbc_path + "/static/";

	std::vector<std::string> filepaths;

	for (const auto & entry : fs::directory_iterator(dyn_path))
        filepaths.push_back(entry.path().u8string());
	for (const auto& entry: fs::directory_iterator(stat_path))
		filepaths.push_back(entry.path().u8string());
	
	// TODO add mapping

	// insert vertex first.
	std::map<std::string, std::map<long,long>*> labelMap;	// temporary here


	for (auto filepath: filepaths) {
		std::string filename = fs::path(filepath).stem();
		// based on filetype, parse content
		int numUnderscores = std::count(filename.begin(), filename.end(), '_');
		// filter only nodes
		if (numUnderscores != 2) { continue; }

		std::string vLabel = filename.substr(0, filename.find("_"));
		strConvertToVertexLabelFormat(vLabel);
		// add to id mapping
		std::map<long,long>* ldbcIdToLiveGraphIdMapping = new std::map<long,long>();
		labelMap[vLabel] = ldbcIdToLiveGraphIdMapping;

		std::ifstream file(filepath);
		long vid_start = graph.get_max_vertex_id(); // actually its +1

		long countryMin = LONG_MAX;
		long countryMax = -1;
		long continentMin = LONG_MAX;
		long continentMax = -1;
		long cityMin = LONG_MAX;
		long cityMax = -1;
		
		long univMin = LONG_MAX;
		long univMax = -1;
		long compMin = LONG_MAX;
		long compMax = -1;

		if(file.is_open()) {
			std::string line;
			bool headerBypassed = 0;
			while(std::getline(file, line)) {
				if( !headerBypassed ) {
					headerBypassed = true;
					continue;
				}
				// get ldbcid
				long ldbcId = std::stol(line.substr(0, line.find("|")));
				// insert
				long liveGraphId = txn.new_vertex(false);
				// handle labelset
				std::string lineToInsert = line;
				if( vLabel.compare("Place")==0 ) {
					std::string secondLabel = line.substr(line.rfind("|")+1, line.length() - line.rfind("|")-1 );
					lineToInsert = line.substr(0, line.rfind("|"));
					//std::cout << lineToInsert << " " << secondLabel << std::endl;
					if( secondLabel.compare("Country") == 0) {
						countryMin = std::min(countryMin, liveGraphId);
						countryMax = std::max(countryMax, liveGraphId);
					} else if (secondLabel.compare("City") == 0) {
						cityMin = std::min(cityMin, liveGraphId);
						cityMax = std::max(cityMax, liveGraphId);
					} else { // Continent
						continentMin = std::min(continentMin, liveGraphId);
						continentMax = std::max(continentMax, liveGraphId);
					}

				} else if (vLabel.compare("Organisation")==0) {
					// different secondlabel here (comes second)
					std::string fromSecond = line.substr(line.find("|")+1, line.length());
					std::string secondLabel = fromSecond.substr(0, fromSecond.find("|"));
					std::string afterProperty = fromSecond.substr(fromSecond.find("|")+1, fromSecond.length());
					
					// re-join
					lineToInsert = std::to_string(ldbcId) + "|"+ afterProperty;
					// std::cout << lineToInsert << " " << secondLabel << std::endl;
					if (secondLabel.compare("Company") == 0) {
						compMin = std::min(compMin, liveGraphId);
						compMax = std::max(compMax, liveGraphId);
					} else { // University
						univMin = std::min(univMin, liveGraphId);
						univMax = std::max(univMax, liveGraphId);
					}
				} else {	// normal case

				}
				// insert data
				txn.put_vertex(liveGraphId, std::string_view(lineToInsert));
				// add to ldbc mapping
				labelMap[vLabel]->insert(std::make_pair(ldbcId, liveGraphId));
			}
			file.close();
		}
		// finally, record to vid range mapping in the catalog
		if( vLabel.compare("Place")==0 ) {
			LabelSet vs1, vs2, vs3;
			vs1.insert(vLabel);
			vs1.insert("Country");
			catalog.vertexLabelSetToLGRangeMapping.push_back(
				std::pair( vs1, std::pair(countryMin, countryMax) )
			);
			vs2.insert(vLabel);
			vs2.insert("Continent");
			catalog.vertexLabelSetToLGRangeMapping.push_back(
				std::pair( vs2, std::pair(continentMin, continentMax) )
			);
			vs3.insert(vLabel);
			vs3.insert("City");
			catalog.vertexLabelSetToLGRangeMapping.push_back(
				std::pair( vs3, std::pair(cityMin, cityMax) )
			);
		} else if( vLabel.compare("Organisation")==0 ) {
			LabelSet vs1, vs2;
			vs1.insert(vLabel);
			vs1.insert("Company");
			catalog.vertexLabelSetToLGRangeMapping.push_back(
				std::pair( vs1, std::pair(compMin, compMax) )
			);
			vs2.insert(vLabel);
			vs2.insert("University");
			catalog.vertexLabelSetToLGRangeMapping.push_back(
				std::pair( vs2, std::pair(univMin, univMax) )
			);
		} else {
			LabelSet vLabelSet;
			vLabelSet.insert(vLabel);
			catalog.vertexLabelSetToLGRangeMapping.push_back( std::pair(vLabelSet, std::pair(vid_start, graph.get_max_vertex_id()-1)));
		}
	}

	// insert edges
	int currentEdgeLabelId = 0;

	for (auto filepath: filepaths) {
		std::string filename = fs::path(filepath).stem();
		// based on filetype, parse content
		int numUnderscores = std::count(filename.begin(), filename.end(), '_');
		// filter only edges
		if (numUnderscores != 4) { continue; }

		int prevUnderScoreIdx;
		std::string srcLabel = filename.substr(0, filename.find("_"));
		prevUnderScoreIdx = filename.find("_");
		strConvertToVertexLabelFormat(srcLabel);
			
		std::string edgeLabel = filename.substr(prevUnderScoreIdx+1, filename.find("_", prevUnderScoreIdx+1) - prevUnderScoreIdx-1);
		prevUnderScoreIdx = filename.find("_", prevUnderScoreIdx+1);
		strConvertToEdgeLabelFormat(edgeLabel);

		std::string dstLabel = filename.substr(prevUnderScoreIdx+1, filename.find("_", prevUnderScoreIdx+1) - prevUnderScoreIdx-1);
		strConvertToVertexLabelFormat(dstLabel);
		
		LabelSet edgeLabelSet;
		edgeLabelSet.insert(edgeLabel);
		catalog.edgeLabelSetToLGEdgeLabelMapping.push_back(
			std::pair(edgeLabelSet, currentEdgeLabelId)	
		);

		// increment
		currentEdgeLabelId++;
	}

	// end transaction
	txn.commit(true);


	// TODO print graph size
	// check there is correct sizes.
	
}