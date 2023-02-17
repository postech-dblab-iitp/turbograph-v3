#pragma once

#include <unordered_set>
#include <string>
#include <iostream>
#include <vector>
#include <tuple>
#include <map>

namespace duckdb {

struct LogicalType;

enum class StoreAPIResult {
	OK,
	DONE,
	ERROR
};

typedef long VertexID;
typedef long EdgeID;

typedef std::vector<std::string> PropertyKeys;

enum class LoadAdjListOption {
	NONE,
	OUTGOING,
	INCOMING,
	BOTH
};

enum class ExpandDirection {
	OUTGOING,
	INCOMING,
	BOTH
};

class LabelSet {

public:
	LabelSet() {}
	LabelSet(std::string e1) { this->insert(e1); }
	LabelSet(std::string e1, std::string e2) { this->insert(e1); this->insert(e1);  }
	LabelSet(std::string e1, std::string e2, std::string e3 ) { this->insert(e1); this->insert(e2); this->insert(e3); }
	void insert(std::string input);
	// TODO support multiple insert at once by supporting variadic template

	bool isSupersetOf(const LabelSet& elem);
	bool contains(const std::string st);
	size_t size() { return this->data.size(); }

	friend bool operator==(const LabelSet lhs, const LabelSet rhs);
	friend std::ostream& operator<<(std::ostream& os, const LabelSet& obj);

public:
	std::unordered_set<std::string> data;
};

enum class CypherValueType {
	// non-nested
	DATA,	// TODO need to be specified more
	ID,
	RANGE,
	// nested
	NODE,
	EDGE,
	PATH
};

class CypherSchema{

public:
	// insert
	void addNode(std::string name);
	// node range type
	void addRange(std::string name);
	void addPropertyIntoNode(std::string nodeName, std::string propName, duckdb::LogicalType type);
	void addColumn(std::string attrName, duckdb::LogicalType type);

	void addEdge(std::string name);
	void addPropertyIntoEdge(std::string edgeName, std::string propName, duckdb::LogicalType type);

	// Get CypherType of column
	CypherValueType getCypherType(std::string name) const;
	// Get logicaltype of non-nested column
	duckdb::LogicalType getType(std::string name) const;

	//! Get DuckDB Types of this CypherSchema
	std::vector<duckdb::LogicalType> getTypes() const;
	//! Get Partial DuckDB Types of this CypherSchema given key
	std::vector<duckdb::LogicalType> getTypesOfKey(std::string name) const;

	CypherSchema getSubSchemaOfKey(std::string name) const;
	int getColIdxOfKey(std::string name) const;
	std::vector<int> getColumnIndicesForResultSet() const;
	std::string toString() const;

	/* TODO S62 temporary!!!!!! - we do not track attribute info with CypherSchema from now*/
	/* this is temporary DS to not change CypherPhysicalOperator interface */
	//! TODO S62 this is a temporary to -> CypherSchema should be disabled by now
	void setStoredTypes(std::vector<duckdb::LogicalType> types);
	std::vector<duckdb::LogicalType> getStoredTypes();
	std::vector<duckdb::LogicalType> stored_types;
	/* TODO S62 temporary!!!!!! */
	
	std::vector<std::tuple<std::string, CypherValueType, duckdb::LogicalType>> attrs;
	std::map<std::string, CypherSchema> nestedAttrs;

	

};
}