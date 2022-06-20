#include "typedef.hpp"
#include <iostream>

void LabelSet::insert(std::string input) {
	this->data.insert(input);
}

std::ostream& operator<<(std::ostream& os, const LabelSet& obj) {
	
	os << "LabelSet(";
	for(auto item: obj.data) {
		os << item << ",";
	}
	os << ")";
    return os;
}