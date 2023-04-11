#include "typedef.hpp"
#include <iostream>
#include <algorithm>
#include <map>

#include "common/types.hpp"

#include "icecream.hpp"	

namespace duckdb {
void LabelSet::insert(std::string input) {
	this->data.insert(input);
}

//! when a is superset of b, a contains all elems of b, which leads to smaller intersection in label hierarchy
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

CypherValueType CypherSchema::getCypherType(std::string name) const {
	for( auto& attr: attrs) {
		auto n = std::get<0>(attr);
		auto cypherType = std::get<1>(attr);
		if ( n.compare(name) == 0 ) {
			return cypherType;
		}
	}
	D_ASSERT(false);
}
duckdb::LogicalType CypherSchema::getType(std::string name) const {
	for( auto& attr: attrs) {
		auto n = std::get<0>(attr);
		auto duckDbType = std::get<2>(attr);
		if ( n.compare(name) == 0 ) {
			// duckdbtype should not be invalid
			return duckDbType ;
		}
	}
	D_ASSERT(false);
}

std::vector<duckdb::LogicalType> CypherSchema::getTypes() const {
	std::vector<duckdb::LogicalType> result;
	for( auto& attr: attrs) {
		auto name = std::get<0>(attr);
		auto cypherType = std::get<1>(attr);
		switch( cypherType ) {
			case CypherValueType::DATA:
			case CypherValueType::ID: {
				result.push_back( std::get<2>(attr) );
				break;
			}
			case CypherValueType::NODE:
			case CypherValueType::RANGE:
			case CypherValueType::EDGE:
			case CypherValueType::PATH: {
				// recursive seek
				std::vector<duckdb::LogicalType> innerResult = nestedAttrs.find(name)->second.getTypes();
				for( auto& r: innerResult ) {
					result.push_back(r);
				}
				break;
			}
			default: {
				assert( 0 && "wrong type");
			}	
		}
	}
	return result;
}

void CypherSchema::addNode(std::string name) {
	
	CypherSchema nodeSchema;
	nodeSchema.attrs.push_back(
		std::make_tuple("_id", CypherValueType::ID, LogicalType(LogicalTypeId::ID))
	);
	// set node info on attrs
	attrs.push_back( std::make_tuple(name, CypherValueType::NODE, LogicalType(LogicalTypeId::INVALID)) );
	// set node details
	nestedAttrs[name] = nodeSchema;
}

void CypherSchema::addEdge(std::string name) {

	CypherSchema edgeSchema;
	edgeSchema.attrs.push_back(
		std::make_tuple("_id", CypherValueType::ID, LogicalType(LogicalTypeId::ID))
	);
	// set edges info on attrs
	attrs.push_back( std::make_tuple(name, CypherValueType::EDGE, LogicalType(LogicalTypeId::INVALID)) );
	// set node details
	nestedAttrs[name] = edgeSchema;
}

void CypherSchema::addRange(std::string name) {

	CypherSchema rangeSchema;
	// insert two columns => adj list range : [_sid, _eid) // startid, endid
	rangeSchema.attrs.push_back(
		std::make_tuple("_sid", CypherValueType::ID, LogicalType(LogicalTypeId::ID))
	);
	rangeSchema.attrs.push_back(
		std::make_tuple("_eid", CypherValueType::ID, LogicalType(LogicalTypeId::ID))
	);
	// set node info on attrs
	attrs.push_back( std::make_tuple(name, CypherValueType::RANGE, LogicalType(LogicalTypeId::INVALID)) );
	nestedAttrs[name] = rangeSchema;
}

void CypherSchema::addPropertyIntoNode(std::string nodeName, std::string propName, duckdb::LogicalType type) {
	nestedAttrs[nodeName].attrs.push_back(
		std::make_tuple(propName, CypherValueType::DATA, type)
	);
}

void CypherSchema::addPropertyIntoEdge(std::string edgeName, std::string propName, duckdb::LogicalType type) {
	nestedAttrs[edgeName].attrs.push_back(
		std::make_tuple(propName, CypherValueType::DATA, type)
	);
}

void CypherSchema::addColumn(std::string attrName, duckdb::LogicalType type) {
	attrs.push_back( std::make_tuple(attrName, CypherValueType::DATA, type) );
}

std::vector<duckdb::LogicalType> CypherSchema::getTypesOfKey(std::string name) const {
	if( nestedAttrs.find(name) != nestedAttrs.end()) {
 		return nestedAttrs.find(name)->second.getTypes();
	}
	return std::vector<duckdb::LogicalType>();
}

CypherSchema CypherSchema::getSubSchemaOfKey(std::string name) const {
	if( nestedAttrs.find(name) != nestedAttrs.end()) {
 		return nestedAttrs.find(name)->second;
	}
}

int CypherSchema::getColIdxOfKey(std::string key) const {

	int result = 0;
	for( auto& attr: attrs) {
		auto name = std::get<0>(attr);
		auto cypherType = std::get<1>(attr);
		switch( cypherType ) {
			case CypherValueType::RANGE:
			case CypherValueType::NODE:
			case CypherValueType::PATH:
			case CypherValueType::EDGE:
				if( name.compare(key) == 0 ) {
					return result;
				} else {
					result += nestedAttrs.find(name)->second.attrs.size();
				}
				break;
			default:
				result += 1;
		}
	}
	// should never be here
	assert(0);
}

std::string CypherSchema::toString() const{

	std::string result;
	result += "(";
	for( auto& attr: attrs) {
		auto name = std::get<0>(attr);
		result += name;
		result += ":";
		// Type (recurse)
		auto cypherType = std::get<1>(attr);
		switch( cypherType ) {
			case CypherValueType::DATA: {
				duckdb::LogicalType dataType = std::get<2>(attr);
				result += dataType.ToString();
				break;
			}
			case CypherValueType::ID: {
				result += "ID";
				break;
			}
			case CypherValueType::NODE: {
				result += "NODE";
				result += nestedAttrs.find(name)->second.toString();
				break;
			}
			case CypherValueType::EDGE: {
				result += "EDGE";
				result += nestedAttrs.find(name)->second.toString();
				break;
			}
			case CypherValueType::PATH: {
				assert(0);
				result += "PATH"; break;
			}
			case CypherValueType::RANGE: {
				result += "RANGE"; break;
			}
		}
		result += ", ";
	}
	// result.pop_back(); result.pop_back(); // delete last comma // XXX bad_alloc bug here
	result += ")";
	return result;
}

std::vector<int> CypherSchema::getColumnIndicesForResultSet() const {

	std::vector<int> result;
	int curIdx = 0;
	for( auto& attr: attrs) {
		auto name = std::get<0>(attr);
		auto cypherType = std::get<1>(attr);
		switch( cypherType ) {
			case CypherValueType::NODE:
			case CypherValueType::PATH:
			case CypherValueType::EDGE:
				for( auto& in_attr: nestedAttrs.find(name)->second.attrs) {
					auto in_type = std::get<1>(in_attr);
					switch( in_type ) {
						case CypherValueType::DATA: {
							result.push_back(curIdx);
							break;
						}
						default:	// in 2nd level, rest are all invalid.
							break;
					}
					curIdx += 1;
				}
				curIdx -= 1;
				break;
			case CypherValueType::DATA:
				result.push_back(curIdx);
				break;
			default:
				// abaondon
				D_ASSERT(false && "cannot print this type as resulttype");
				break;
		}
		curIdx += 1;
	}
	return result;
}

void CypherSchema::setStoredTypes(std::vector<duckdb::LogicalType> types) {
	for( auto& t: types) {
		stored_types.push_back(t);
	}
}

void CypherSchema::setStoredColumnNames(std::vector<std::string> &names) {
	for( auto& t: names) {
		stored_column_names.push_back(t);
	}
}

std::vector<duckdb::LogicalType> CypherSchema::getStoredTypes() {
	return stored_types;
}

std::vector<string> CypherSchema::getStoredColumnNames() {
	return stored_column_names;
}


std::string CypherSchema::printStoredTypes() {
	std::string result = "(";
	for(duckdb::LogicalType& type: stored_types) {
		result += type.ToString();
		result += ", ";
	}
	result += ")";
	return result;
}

std::string CypherSchema::printStoredColumnAndTypes() {
	if( stored_types.size() != stored_column_names.size() ) {
		return printStoredTypes();
	}
	std::string result = "(";
	for(int idx = 0; idx < stored_types.size(); idx++) {
		result += stored_column_names[idx];
		result += ":";
		result += stored_types[idx].ToString();
		result += ", ";
	}
	result += ")";
	return result;
}


}