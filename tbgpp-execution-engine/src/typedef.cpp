#include "typedef.hpp"
#include <iostream>
#include <algorithm>
#include <map>

void LabelSet::insert(std::string input) {
	this->data.insert(input);
}

bool LabelSet::isSupersetOf(const LabelSet& elem) {

	// if size bigger, always false
	if( elem.data.size() > this->data.size() ) return false;
	// if same or small, check if all members exist.
	for( const auto& item: elem.data ) {
		if( this->data.find(item) == this->data.end() ) {
			return false;
		}
	}
	return true;
}

bool LabelSet::contains(const std::string st) {
	return this->data.find(st) != this->data.end();
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

std::vector<duckdb::LogicalType> CypherSchema::getTypes() {
	std::vector<duckdb::LogicalType> result;
	for( auto& attr: attrs) {
		auto name = std::get<0>(attr);
		auto cypherType = std::get<1>(attr);
		switch( cypherType ) {
			case CypherValueType::DATA:
			case CypherValueType::ID:
			case CypherValueType::ADJLIST: {
				result.push_back( std::get<2>(attr) );
				break;
			}
			case CypherValueType::NODE:
			case CypherValueType::EDGE:
			case CypherValueType::PATH: {
				// recursive seek
				std::vector<duckdb::LogicalType> innerResult = nestedAttrs[name].getTypes();
				for( auto& r: innerResult ) {
					result.push_back(r);
				}
			}
			default: {
				assert( 0 && "wrong type");
			}	
		}
	}
	return result;
}
