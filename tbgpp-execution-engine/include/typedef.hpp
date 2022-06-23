#pragma once

#include <unordered_set>
#include <string>
#include <iostream>
#include <vector>
#include <tuple>
#include <map>

#include "duckdb/common/types.hpp"

enum class StoreAPIResult {
	OK,
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

class LabelSet {

public:
	LabelSet() {}
	void insert(std::string input);
	// TODO support multiple insert at once by supporting variadic template

	bool isSupersetOf(const LabelSet& elem);
	bool contains(const std::string st);
	size_t size() { return this->data.size(); }

	friend bool operator==(const LabelSet lhs, const LabelSet rhs);
	friend std::ostream& operator<<(std::ostream& os, const LabelSet& obj);

private:
	std::unordered_set<std::string> data;
};

enum class CypherValueType {
	// non-nested
	DATA,	// TODO need to be specified more
	ID,
	ADJLIST,
	// nested
	NODE,
	EDGE,
	PATH
};

class CypherSchema{

public:
	std::vector<duckdb::LogicalType> getTypes();
	// TODO add print function.
	// TODO add insert function.

private:
	std::vector<std::tuple<std::string, CypherValueType, duckdb::LogicalType>> attrs;
	std::map<std::string, CypherSchema> nestedAttrs;
};
