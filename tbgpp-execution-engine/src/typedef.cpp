#include "typedef.hpp"
#include <iostream>

void LabelSet::insert(std::string input) {
	this->data.insert(input);
}

bool LabelSet::contains(const LabelSet& elem) {
	return true;

}

std::ostream& operator<<(std::ostream& os, const LabelSet& obj) {
	os << "LabelSet(";
	for(auto item: obj.data) {
		os << item << ",";
	}
	os << ")";
    return os;
}

bool operator==(const LabelSet lhs, const LabelSet rhs) {
	// This is a comparison between 'unordered' sets, thus different ordering will return true.
	return lhs.data == rhs.data;
}