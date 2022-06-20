#pragma once

#include <unordered_set>
#include <string>
#include <iostream>

// typedef std::unordered_set<std::string> LabelSet;

typedef long VertexID;
typedef long EdgeID;

class LabelSet {

public:
	LabelSet() {}
	void insert(std::string input);

	friend std::ostream& operator<<(std::ostream& os, const LabelSet& obj);

private:
	std::unordered_set<std::string> data;

};

