#pragma once

#include <unordered_set>
#include <string>
#include <iostream>
#include <vector>

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
	// TODO need to improve search performance

public:
	LabelSet() {}
	void insert(std::string input);
	
	bool contains(const LabelSet& elem);
	// TODO contains

	friend bool operator==(const LabelSet lhs, const LabelSet rhs);
	friend std::ostream& operator<<(std::ostream& os, const LabelSet& obj);

private:
	std::unordered_set<std::string> data;
};

