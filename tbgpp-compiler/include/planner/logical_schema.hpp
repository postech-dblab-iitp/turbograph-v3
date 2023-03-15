#pragma once

#include <string>
#include <vector>
#include <utility>
#include <cassert>
#include <set>

#include "assert.hpp"

using namespace std;

namespace s62 {

class LogicalSchema {

public:
	LogicalSchema() {
		bound_nodes = set<string>();
		bound_edges = set<string>();
	}
	~LogicalSchema() { }

	void appendSchema(LogicalSchema* sch1) {
		// TODO ASSERT key does not collapse
		// schema
		auto& sch = *sch1;
		for( auto& prop: sch.schema) {
			this->schema.push_back(prop);
		}
		// bound variables
		for( auto& it: sch.bound_nodes) {
			this->bound_nodes.insert(it);
		}
		for( auto& it: sch.bound_edges) {
			this->bound_edges.insert(it);
		}
	}
	void copySchema(LogicalSchema *other, vector<uint64_t> sel_vector) {
		for (int i = 0; i < sel_vector.size(); i++) {
			D_ASSERT(sel_vector[i] < other->size());
			this->schema.push_back(other->getSchema()[sel_vector[i]]);
		}
	}
	void appendNodeProperty(string& k1, string& k2) {
		appendKey(k1, k2, true, false);
	}
	void appendEdgeProperty(string& k1, string& k2) {
		appendKey(k1, k2, false, true);
	}
	void appendColumn(string& k1) {
		string none = "";
		appendKey(k1, none, false, false);
	}
	
	uint64_t getNumPropertiesOfKey(string& k1) {
		uint64_t cnt = 0;
		for( auto& col: schema) {
			if( col.first == k1 ) { cnt++; }
		}
		return cnt;
	}
	uint64_t getIdxOfKey(string& k1, string& k2) {
		bool found = false;
		uint64_t found_idx = -1;
		for(int idx = 0; idx < schema.size(); idx++) {
			auto& col = schema[idx];
			if(col.first == k1 && col.second == k2) {
				found = true;
				found_idx = idx;
			}
		}
		D_ASSERT( found == true );
		return found_idx;
	}
	bool isNodeBound(string k1) { return bound_nodes.size() > 0 && (bound_nodes.find(k1) != bound_nodes.end()); }
	bool isEdgeBound(string k1) { return bound_edges.size() > 0 && (bound_edges.find(k1) != bound_edges.end()); }
	uint64_t size() { return schema.size(); }
	vector<pair<string, string>> &getSchema() {
		return schema;
	}
	void clear() {
		schema.clear();
		bound_edges.clear();
		bound_nodes.clear();
	}
	bool isEmpty() {
		return schema.size() == 0 && bound_edges.size() == 0 && bound_nodes.size() == 0;
	}
	std::string toString() {
		std::string output = "SCHEMA => \n";
		for(int idx=0; idx < schema.size(); idx++) {
			auto& sch = schema[idx];
			output += " - ["  + std::to_string(idx) + "]" + sch.first;
			if (sch.second != "") {
				output += "." + sch.second;
			}
			output += "\n";
		}
		return output;
	}

private:

/* Append to the last column of the schema */
	void appendKey(string& k1, string& k2, bool is_node, bool is_edge) {

		// TODO temporary resolve me!
		size_t dot_pos = k2.find_last_of(".");
		// make sure the poisition is valid
		if (dot_pos != string::npos)
			k2 = k2.substr(dot_pos+1);

		D_ASSERT( k2 != "" );
		D_ASSERT( !(is_node && is_edge) );
		if(is_node) { bound_nodes.insert(k1); }
		else if(is_edge) { bound_edges.insert(k1); }
		else { D_ASSERT(k2 != "_id"); }	// property with _id indicates _node or _id

		schema.push_back(make_pair(k1, k2));
	}

private:

	vector<pair<string, string>> schema;
	set<string> bound_nodes;
	set<string> bound_edges;
};

}